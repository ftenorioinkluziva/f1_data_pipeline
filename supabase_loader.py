import asyncio
import asyncpg
from datetime import datetime
from typing import Dict, List, Any, Optional

from loguru import logger

from models import Driver, Session, LapData, Position, TelemetryData, RaceControl, Weather
from config_supabase import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

class SupabaseLoader:
    """Carrega dados da F1 no Supabase via conexão PostgreSQL"""
    
    def __init__(self):
        self.pool = None
        self.conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    async def connect(self) -> None:
        """Estabelece conexão direta com o banco de dados Supabase"""
        try:
            logger.info(f"Conectando diretamente ao PostgreSQL do Supabase em {DB_HOST}:{DB_PORT}/{DB_NAME}...")
            self.pool = await asyncpg.create_pool(
                dsn=self.conn_string,
                min_size=1,
                max_size=10,
                ssl="require"  # Supabase requer SSL
            )
            logger.info("Conexão estabelecida com sucesso")
            
            # Verifica se as tabelas existem, caso contrário, cria
            await self._ensure_tables_exist()
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao Supabase: {e}")
            # Mask password in log
            safe_conn_string = self.conn_string.replace(DB_PASSWORD, "********") if DB_PASSWORD else self.conn_string
            logger.error(f"Connection string: {safe_conn_string}")
            raise
    
    async def disconnect(self) -> None:
        """Fecha a conexão com o banco de dados"""
        if self.pool:
            await self.pool.close()
            logger.info("Conexão com o Supabase fechada")
    
    async def _ensure_tables_exist(self) -> None:
        """Verifica se as tabelas necessárias existem no Supabase, caso contrário, cria-as"""
        async with self.pool.acquire() as conn:
            # No Supabase, precisamos usar o esquema public explicitamente
            
            # Tabela de sessões
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.sessions (
                    id SERIAL PRIMARY KEY,
                    session_key INTEGER UNIQUE NOT NULL,
                    meeting_key INTEGER NOT NULL,
                    name VARCHAR(255),
                    date TIMESTAMP WITH TIME ZONE,
                    circuit VARCHAR(255),
                    type VARCHAR(50),
                    location VARCHAR(255),
                    country_name VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de pilotos
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.drivers (
                    id SERIAL PRIMARY KEY,
                    driver_number INTEGER UNIQUE NOT NULL,
                    name VARCHAR(255),
                    team VARCHAR(255),
                    country_code VARCHAR(10),
                    team_color VARCHAR(10),
                    first_name VARCHAR(100),
                    last_name VARCHAR(100),
                    short_name VARCHAR(10),
                    headshot_url TEXT,
                    broadcast_name VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de dados de voltas
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.lap_data (
                    id SERIAL PRIMARY KEY,
                    driver_number INTEGER NOT NULL,
                    lap_number INTEGER NOT NULL,
                    lap_time FLOAT,
                    sector_1_time FLOAT,
                    sector_2_time FLOAT,
                    sector_3_time FLOAT,
                    speed_trap INTEGER,
                    timestamp TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (driver_number, lap_number)
                )
            ''')
            
            # Tabela de posições
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.positions (
                    id SERIAL PRIMARY KEY,
                    driver_number INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de telemetria
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.telemetry (
                    id SERIAL PRIMARY KEY,
                    driver_number INTEGER NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    speed INTEGER,
                    rpm INTEGER,
                    gear INTEGER,
                    throttle INTEGER,
                    brake INTEGER,
                    drs INTEGER,
                    x FLOAT,
                    y FLOAT,
                    z FLOAT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de controle de corrida
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.race_control (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    message TEXT NOT NULL,
                    category VARCHAR(50),
                    flag VARCHAR(50),
                    driver_number INTEGER,
                    scope VARCHAR(50),
                    sector INTEGER,
                    lap_number INTEGER,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de dados meteorológicos
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS public.weather (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    air_temp FLOAT,
                    track_temp FLOAT,
                    humidity FLOAT,
                    pressure FLOAT,
                    wind_speed FLOAT,
                    wind_direction INTEGER,
                    rainfall BOOLEAN,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Cria índices para melhorar a performance
            try:
                # Índice de timestamp para posições
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS positions_timestamp_idx 
                    ON public.positions (timestamp)
                ''')
                
                # Índice de timestamp para telemetria
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS telemetry_timestamp_idx 
                    ON public.telemetry (timestamp)
                ''')
                
                # Índice de driver_number para lap_data
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS lap_data_driver_number_idx 
                    ON public.lap_data (driver_number)
                ''')
                
                logger.info("Índices criados com sucesso")
            except Exception as e:
                logger.error(f"Erro ao criar índices: {e}")
            
            logger.info("Verificação e criação de tabelas concluída")
    
    async def load_batch(self, batch_data: Dict[str, List]) -> None:
        """Carrega um lote de dados no Supabase"""
        if not self.pool:
            logger.error("Conexão com o banco de dados não inicializada")
            return
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Carrega sessões
                if batch_data.get('sessions'):
                    await self._load_sessions(conn, batch_data['sessions'])
                
                # Carrega drivers
                if batch_data.get('drivers'):
                    await self._load_drivers(conn, batch_data['drivers'])
                
                # Carrega dados de volta
                if batch_data.get('lap_data'):
                    await self._load_lap_data(conn, batch_data['lap_data'])
                
                # Carrega posições
                if batch_data.get('positions'):
                    await self._load_positions(conn, batch_data['positions'])
                
                # Carrega telemetria
                if batch_data.get('telemetry'):
                    await self._load_telemetry(conn, batch_data['telemetry'])
                
                # Carrega controle de corrida
                if batch_data.get('race_control'):
                    await self._load_race_control(conn, batch_data['race_control'])
                
                # Carrega dados meteorológicos
                if batch_data.get('weather'):
                    await self._load_weather(conn, batch_data['weather'])
    
    async def _load_sessions(self, conn, sessions: List[Session]) -> None:
        """Carrega dados de sessão no Supabase"""
        if not sessions:
            return
            
        for session in sessions:
            try:
                await conn.execute('''
                    INSERT INTO public.sessions (
                        session_key, meeting_key, name, date, circuit, 
                        type, location, country_name
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (session_key) DO UPDATE SET
                        meeting_key = EXCLUDED.meeting_key,
                        name = EXCLUDED.name,
                        date = EXCLUDED.date,
                        circuit = EXCLUDED.circuit,
                        type = EXCLUDED.type,
                        location = EXCLUDED.location,
                        country_name = EXCLUDED.country_name
                ''', 
                    session.session_key, session.meeting_key, session.name, 
                    session.date, session.circuit, getattr(session, 'type', None),
                    getattr(session, 'location', None), getattr(session, 'country_name', None)
                )
            except Exception as e:
                logger.error(f"Erro ao inserir sessão {session.session_key}: {e}")
    
    async def _load_drivers(self, conn, drivers: List[Driver]) -> None:
        """Carrega dados de pilotos no Supabase"""
        if not drivers:
            return
            
        for driver in drivers:
            try:
                await conn.execute('''
                    INSERT INTO public.drivers (
                        driver_number, name, team, country_code, team_color,
                        first_name, last_name, short_name, headshot_url, broadcast_name
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (driver_number) DO UPDATE SET
                        name = EXCLUDED.name,
                        team = EXCLUDED.team,
                        country_code = EXCLUDED.country_code,
                        team_color = EXCLUDED.team_color,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        short_name = EXCLUDED.short_name,
                        headshot_url = EXCLUDED.headshot_url,
                        broadcast_name = EXCLUDED.broadcast_name,
                        updated_at = CURRENT_TIMESTAMP
                ''', 
                    driver.driver_number, driver.name, driver.team, driver.country_code,
                    getattr(driver, 'team_color', None), getattr(driver, 'first_name', None),
                    getattr(driver, 'last_name', None), getattr(driver, 'short_name', None),
                    getattr(driver, 'headshot_url', None), getattr(driver, 'broadcast_name', None)
                )
            except Exception as e:
                logger.error(f"Erro ao inserir piloto {driver.driver_number}: {e}")
    
    async def _load_lap_data(self, conn, lap_data_list: List[LapData]) -> None:
        """Carrega dados de voltas no Supabase"""
        if not lap_data_list:
            return
            
        for lap_data in lap_data_list:
            try:
                await conn.execute('''
                    INSERT INTO public.lap_data (
                        driver_number, lap_number, lap_time, sector_1_time,
                        sector_2_time, sector_3_time, speed_trap, timestamp
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (driver_number, lap_number) DO UPDATE SET
                        lap_time = COALESCE(EXCLUDED.lap_time, lap_data.lap_time),
                        sector_1_time = COALESCE(EXCLUDED.sector_1_time, lap_data.sector_1_time),
                        sector_2_time = COALESCE(EXCLUDED.sector_2_time, lap_data.sector_2_time),
                        sector_3_time = COALESCE(EXCLUDED.sector_3_time, lap_data.sector_3_time),
                        speed_trap = COALESCE(EXCLUDED.speed_trap, lap_data.speed_trap),
                        timestamp = EXCLUDED.timestamp
                ''', 
                    lap_data.driver_number, lap_data.lap_number, lap_data.lap_time,
                    lap_data.sector_1_time, lap_data.sector_2_time, lap_data.sector_3_time,
                    lap_data.speed_trap, lap_data.timestamp
                )
            except Exception as e:
                logger.error(f"Erro ao inserir lap data para piloto {lap_data.driver_number}, volta {lap_data.lap_number}: {e}")
    
    async def _load_positions(self, conn, positions: List[Position]) -> None:
        """Carrega dados de posição no Supabase"""
        if not positions:
            return
            
        # Para posições, usamos inserção em massa para melhor performance
        try:
            values = [(p.driver_number, p.position, p.timestamp) for p in positions]
            await conn.executemany('''
                INSERT INTO public.positions (driver_number, position, timestamp)
                VALUES ($1, $2, $3)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir posições em lote: {e}")
    
    async def _load_telemetry(self, conn, telemetry_list: List[TelemetryData]) -> None:
        """Carrega dados de telemetria no Supabase"""
        if not telemetry_list:
            return
            
        # Para telemetria, usamos inserção em massa para melhor performance
        try:
            values = [(
                t.driver_number, t.timestamp, t.speed, t.rpm, t.gear,
                t.throttle, t.brake, t.drs, t.x, t.y, t.z
            ) for t in telemetry_list]
            
            # Dividir em lotes menores para evitar sobrecarga
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i:i+batch_size]
                
                await conn.executemany('''
                    INSERT INTO public.telemetry (
                        driver_number, timestamp, speed, rpm, gear,
                        throttle, brake, drs, x, y, z
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ''', batch)
                
                logger.debug(f"Lote de telemetria inserido: {len(batch)} registros")
                
        except Exception as e:
            logger.error(f"Erro ao inserir telemetria em lote: {e}")
    
    async def _load_race_control(self, conn, race_control_list: List[RaceControl]) -> None:
        """Carrega mensagens de controle de corrida no Supabase"""
        if not race_control_list:
            return
            
        try:
            values = [(
                rc.timestamp, rc.message, rc.category, rc.flag,
                rc.driver_number, getattr(rc, 'scope', None),
                getattr(rc, 'sector', None), getattr(rc, 'lap_number', None)
            ) for rc in race_control_list]
            
            await conn.executemany('''
                INSERT INTO public.race_control (
                    timestamp, message, category, flag,
                    driver_number, scope, sector, lap_number
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir race control em lote: {e}")
    
    async def _load_weather(self, conn, weather_list: List[Weather]) -> None:
        """Carrega dados meteorológicos no Supabase"""
        if not weather_list:
            return
            
        try:
            values = [(
                w.timestamp, w.air_temp, w.track_temp, w.humidity,
                w.pressure, w.wind_speed, w.wind_direction, w.rainfall
            ) for w in weather_list]
            
            await conn.executemany('''
                INSERT INTO public.weather (
                    timestamp, air_temp, track_temp, humidity,
                    pressure, wind_speed, wind_direction, rainfall
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir weather em lote: {e}")