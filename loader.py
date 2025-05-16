import asyncio
import asyncpg
from datetime import datetime
from typing import Dict, List, Any, Optional

from loguru import logger

from models import Driver, Session, LapData, Position, TelemetryData, RaceControl, Weather
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

class PostgreSQLLoader:
    """Carrega dados da F1 no PostgreSQL/Supabase"""
    
    def __init__(self):
        self.pool = None
        self.conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    async def connect(self) -> None:
        """Estabelece conexão com o banco de dados"""
        try:
            logger.info(f"Conectando ao PostgreSQL em {DB_HOST}:{DB_PORT}/{DB_NAME}...")
            self.pool = await asyncpg.create_pool(
                dsn=self.conn_string,
                min_size=1,
                max_size=10
            )
            logger.info("Conexão estabelecida com sucesso")
            
            # Verifica se as tabelas existem, caso contrário, cria
            await self._ensure_tables_exist()
            
        except Exception as e:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Fecha a conexão com o banco de dados"""
        if self.pool:
            await self.pool.close()
            logger.info("Conexão com o PostgreSQL fechada")
    
    async def _ensure_tables_exist(self) -> None:
        """Verifica se as tabelas necessárias existem, caso contrário, cria-as"""
        async with self.pool.acquire() as conn:
            # Tabela de sessões
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    session_key INTEGER PRIMARY KEY,
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
                CREATE TABLE IF NOT EXISTS drivers (
                    driver_number INTEGER PRIMARY KEY,
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
                CREATE TABLE IF NOT EXISTS lap_data (
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
                CREATE TABLE IF NOT EXISTS positions (
                    id SERIAL PRIMARY KEY,
                    driver_number INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de telemetria
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
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
                CREATE TABLE IF NOT EXISTS race_control (
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
                CREATE TABLE IF NOT EXISTS weather (
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
            
            logger.info("Verificação e criação de tabelas concluída")
    
    async def load_batch(self, batch_data: Dict[str, List]) -> None:
        """Carrega um lote de dados no PostgreSQL"""
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
        """Carrega dados de sessão no banco de dados"""
        if not sessions:
            return
            
        for session in sessions:
            try:
                await conn.execute('''
                    INSERT INTO sessions (
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
        """Carrega dados de pilotos no banco de dados"""
        if not drivers:
            return
            
        for driver in drivers:
            try:
                await conn.execute('''
                    INSERT INTO drivers (
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
        """Carrega dados de voltas no banco de dados"""
        if not lap_data_list:
            return
            
        for lap_data in lap_data_list:
            try:
                await conn.execute('''
                    INSERT INTO lap_data (
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
        """Carrega dados de posição no banco de dados"""
        if not positions:
            return
            
        # Para posições, inserimos tudo sem verificação de duplicidade
        try:
            values = [(p.driver_number, p.position, p.timestamp) for p in positions]
            await conn.executemany('''
                INSERT INTO positions (driver_number, position, timestamp)
                VALUES ($1, $2, $3)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir posições em lote: {e}")
    
    async def _load_telemetry(self, conn, telemetry_list: List[TelemetryData]) -> None:
        """Carrega dados de telemetria no banco de dados"""
        if not telemetry_list:
            return
            
        # Para telemetria, inserimos tudo sem verificação de duplicidade
        try:
            values = [(
                t.driver_number, t.timestamp, t.speed, t.rpm, t.gear,
                t.throttle, t.brake, t.drs, t.x, t.y, t.z
            ) for t in telemetry_list]
            
            await conn.executemany('''
                INSERT INTO telemetry (
                    driver_number, timestamp, speed, rpm, gear,
                    throttle, brake, drs, x, y, z
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir telemetria em lote: {e}")
    
    async def _load_race_control(self, conn, race_control_list: List[RaceControl]) -> None:
        """Carrega mensagens de controle de corrida no banco de dados"""
        if not race_control_list:
            return
            
        try:
            values = [(
                rc.timestamp, rc.message, rc.category, rc.flag,
                rc.driver_number, getattr(rc, 'scope', None),
                getattr(rc, 'sector', None), getattr(rc, 'lap_number', None)
            ) for rc in race_control_list]
            
            await conn.executemany('''
                INSERT INTO race_control (
                    timestamp, message, category, flag,
                    driver_number, scope, sector, lap_number
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir race control em lote: {e}")
    
    async def _load_weather(self, conn, weather_list: List[Weather]) -> None:
    
        """Carrega dados meteorológicos no banco de dados"""
        if not weather_list:
            return
            
        try:
            values = [(
                w.timestamp, w.air_temp, w.track_temp, w.humidity,
                w.pressure, w.wind_speed, w.wind_direction, w.rainfall
            ) for w in weather_list]
            
            await conn.executemany('''
                INSERT INTO weather (
                    timestamp, air_temp, track_temp, humidity,
                    pressure, wind_speed, wind_direction, rainfall
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir weather em lote: {e}")

    async def _bulk_insert(self, conn, table_name: str, columns: List[str], values_list: List[tuple]) -> None:
        """Realiza inserção em massa para melhor desempenho"""
        if not values_list:
            return
            
        try:
            # Cria a query de inserção em massa
            placeholders = ', '.join([f'(${i * len(columns) + j + 1})' for i in range(len(values_list)) for j in range(len(columns))])
            columns_str = ', '.join(columns)
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES {placeholders}"
            
            # Achata a lista de valores
            flat_values = [val for tup in values_list for val in tup]
            
            # Executa a query
            await conn.execute(query, *flat_values)
        except Exception as e:
            logger.error(f"Erro ao fazer inserção em massa na tabela {table_name}: {e}")