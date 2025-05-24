import asyncio
import asyncpg
from datetime import datetime
from typing import Dict, List, Any, Optional

from loguru import logger

from models import Driver, Session, LapData, Position, TelemetryData, RaceControl, Weather
from config_supabase import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

class SupabaseLoader:
    """Carrega dados da F1 no Supabase via conexão PostgreSQL usando tabelas existentes"""
    
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
            
            # Apenas verifica se as tabelas existem, não as cria
            await self._verify_tables_exist()
            
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
    
    async def _verify_tables_exist(self) -> None:
        """Verifica se as tabelas necessárias existem no Supabase"""
        required_tables = [
            'sessions', 'weather_data', 'session_drivers', 'driver_positions', 
            'car_positions', 'car_telemetry', 'race_control_messages', 'team_radio'
        ]
        
        async with self.pool.acquire() as conn:
            try:
                # Verifica quais tabelas existem
                existing_tables = await conn.fetch('''
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    AND table_name = ANY($1::text[])
                ''', required_tables)
                
                existing_table_names = [row['table_name'] for row in existing_tables]
                logger.info(f"Tabelas encontradas: {existing_table_names}")
                
                # Verifica se todas as tabelas necessárias existem
                missing_tables = set(required_tables) - set(existing_table_names)
                if missing_tables:
                    logger.warning(f"Tabelas não encontradas: {missing_tables}")
                    logger.warning("O pipeline pode falhar se essas tabelas não existirem!")
                else:
                    logger.info("Todas as tabelas necessárias foram encontradas")
                
                # Verifica estrutura específica das tabelas mais importantes
                await self._verify_sessions_table_structure(conn)
                await self._verify_weather_data_table_structure(conn)
                
            except Exception as e:
                logger.error(f"Erro ao verificar estrutura das tabelas: {e}")
    
    async def _verify_sessions_table_structure(self, conn):
        """Verifica se a tabela sessions tem a estrutura esperada"""
        try:
            columns = await conn.fetch('''
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'sessions'
                ORDER BY ordinal_position
            ''')
            
            column_info = {row['column_name']: row['data_type'] for row in columns}
            logger.debug(f"Estrutura da tabela sessions: {column_info}")
            
            # Verifica se os campos essenciais existem
            essential_fields = ['id', 'key', 'name', 'type']
            missing_fields = [field for field in essential_fields if field not in column_info]
            
            if missing_fields:
                logger.error(f"Campos essenciais não encontrados na tabela sessions: {missing_fields}")
            else:
                logger.info("Estrutura da tabela sessions verificada com sucesso")
                
        except Exception as e:
            logger.error(f"Erro ao verificar estrutura da tabela sessions: {e}")
    
    async def _verify_weather_data_table_structure(self, conn):
        """Verifica se a tabela weather_data tem a estrutura esperada"""
        try:
            columns = await conn.fetch('''
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'weather_data'
                ORDER BY ordinal_position
            ''')
            
            column_info = {row['column_name']: row['data_type'] for row in columns}
            logger.debug(f"Estrutura da tabela weather_data: {column_info}")
            
            # Verifica se os campos essenciais existem
            essential_fields = ['id', 'session_id', 'timestamp', 'air_temp', 'track_temp']
            missing_fields = [field for field in essential_fields if field not in column_info]
            
            if missing_fields:
                logger.error(f"Campos essenciais não encontrados na tabela weather_data: {missing_fields}")
            else:
                logger.info("Estrutura da tabela weather_data verificada com sucesso")
                
        except Exception as e:
            logger.error(f"Erro ao verificar estrutura da tabela weather_data: {e}")
    
    async def load_batch(self, batch_data: Dict[str, List]) -> None:
        """Carrega um lote de dados no Supabase usando tabelas existentes"""
        if not self.pool:
            logger.error("Conexão com o banco de dados não inicializada")
            return
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Carrega sessões - usando campos corretos da tabela existente
                if batch_data.get('sessions'):
                    await self._load_sessions(conn, batch_data['sessions'])
                
                # Carrega drivers -> session_drivers
                if batch_data.get('drivers'):
                    await self._load_session_drivers(conn, batch_data['drivers'])
                
                # Carrega dados de volta (não há tabela específica, usar driver_positions se necessário)
                if batch_data.get('lap_data'):
                    logger.info("lap_data não mapeado para tabela específica - ignorando")
                
                # Carrega posições -> driver_positions
                if batch_data.get('positions'):
                    await self._load_driver_positions(conn, batch_data['positions'])
                
                # Carrega telemetria -> car_telemetry
                if batch_data.get('telemetry'):
                    await self._load_car_telemetry(conn, batch_data['telemetry'])
                
                # Carrega controle de corrida -> race_control_messages
                if batch_data.get('race_control'):
                    await self._load_race_control_messages(conn, batch_data['race_control'])
                
                # Carrega dados meteorológicos
                if batch_data.get('weather'):
                    await self._load_weather(conn, batch_data['weather'])
                
                # Carrega posições dos carros (nova funcionalidade)
                if batch_data.get('car_positions'):
                    await self._load_car_positions(conn, batch_data['car_positions'])
    
    async def _load_sessions(self, conn, sessions: List[Session]) -> None:
        """Carrega dados de sessão no Supabase usando estrutura da tabela existente"""
        if not sessions:
            return
            
        for session in sessions:
            try:
                # Usar os campos corretos da tabela sessions
                await conn.execute('''
                    INSERT INTO public.sessions (
                        key, type, name, start_date, race_id,
                        end_date, gmt_offset, path, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (key) DO UPDATE SET
                        type = EXCLUDED.type,
                        name = EXCLUDED.name,
                        start_date = EXCLUDED.start_date,
                        end_date = EXCLUDED.end_date,
                        updated_at = CURRENT_TIMESTAMP
                ''', 
                    session.session_key,  # maps to 'key' field
                    getattr(session, 'type', 'Unknown'),
                    session.name,
                    session.date,  # maps to 'start_date'
                    getattr(session, 'race_id', 1),  # Default race_id, adjust as needed
                    getattr(session, 'end_date', None),
                    getattr(session, 'gmt_offset', None),
                    getattr(session, 'path', None),
                    datetime.now(),
                    datetime.now()
                )
            except Exception as e:
                logger.error(f"Erro ao inserir sessão {session.session_key}: {e}")
    
    async def _load_session_drivers(self, conn, drivers: List[Driver]) -> None:
        """Carrega dados de pilotos na tabela session_drivers do Supabase"""
        if not drivers:
            return
        
        # Nota: Esta função precisa de um session_id para mapear para session_drivers
        # Como não temos session_id no modelo Driver, vamos log um aviso        
        logger.warning("Carregamento de drivers requer session_id específico - funcionalidade não implementada completamente")
        
        for driver in drivers:
            try:
                # Esta query precisa ser ajustada com um session_id real
                await conn.execute('''
                    INSERT INTO public.session_drivers (
                        session_id, driver_number, full_name, broadcast_name, tla,
                        team_name, team_color, first_name, last_name,
                        headshot_url, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (session_id, driver_number) DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        broadcast_name = EXCLUDED.broadcast_name,
                        tla = EXCLUDED.tla,
                        team_name = EXCLUDED.team_name,
                        team_color = EXCLUDED.team_color,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        headshot_url = EXCLUDED.headshot_url,
                        updated_at = CURRENT_TIMESTAMP
                ''', 
                    1,  # session_id placeholder - precisa ser implementado corretamente
                    str(driver.driver_number), 
                    driver.name or '', 
                    getattr(driver, 'broadcast_name', None),
                    getattr(driver, 'short_name', None),
                    driver.team, 
                    getattr(driver, 'team_color', None),
                    getattr(driver, 'first_name', None),
                    getattr(driver, 'last_name', None),
                    getattr(driver, 'headshot_url', None),
                    datetime.now(), 
                    datetime.now()
                )
            except Exception as e:
                logger.error(f"Erro ao inserir piloto {driver.driver_number} na session_drivers: {e}")
    
    async def _load_driver_positions(self, conn, positions: List[Position]) -> None:
        """Carrega dados de posição na tabela driver_positions do Supabase"""
        if not positions:
            return
            
        try:
            # Mapeia para a estrutura da tabela driver_positions
            values = [(
                1,  # session_id placeholder - precisa ser implementado
                p.timestamp.replace(tzinfo=None) if p.timestamp.tzinfo else p.timestamp,  # timestamp without time zone
                str(p.driver_number),
                p.position,
                datetime.now(),
                datetime.now()
            ) for p in positions]
            
            await conn.executemany('''
                INSERT INTO public.driver_positions 
                (session_id, timestamp, driver_number, position, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', values)
            
        except Exception as e:
            logger.error(f"Erro ao inserir posições na driver_positions: {e}")
    
    async def _load_car_telemetry(self, conn, telemetry_list: List[TelemetryData]) -> None:
        """Carrega dados de telemetria na tabela car_telemetry do Supabase"""
        if not telemetry_list:
            return
            
        try:
            values = [(
                t.timestamp.replace(tzinfo=None) if t.timestamp.tzinfo else t.timestamp,  # timestamp without time zone
                t.timestamp.replace(tzinfo=None) if t.timestamp.tzinfo else t.timestamp,  # utc_timestamp
                1,  # session_id placeholder
                str(t.driver_number),
                t.rpm, t.speed, t.gear,
                float(t.throttle) if t.throttle else None,
                float(t.brake) if t.brake else None,
                t.drs,
                datetime.now(),
                datetime.now()
            ) for t in telemetry_list]
            
            # Dividir em lotes menores para evitar sobrecarga
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i:i+batch_size]
                
                await conn.executemany('''
                    INSERT INTO public.car_telemetry (
                        timestamp, utc_timestamp, session_id, driver_number,
                        rpm, speed, gear, throttle, brake, drs,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ''', batch)
                
                logger.debug(f"Lote de telemetria inserido: {len(batch)} registros")
                
        except Exception as e:
            logger.error(f"Erro ao inserir telemetria na car_telemetry: {e}")
    
    async def _load_race_control_messages(self, conn, race_control_list: List[RaceControl]) -> None:
        """Carrega mensagens de controle de corrida na tabela race_control_messages do Supabase"""
        if not race_control_list:
            return
            
        try:
            values = [(
                1,  # session_id placeholder  
                rc.timestamp,  # timestamp with time zone OK
                getattr(rc, 'utc_time', None),
                rc.category,
                rc.message,
                rc.flag,
                getattr(rc, 'scope', None),
                getattr(rc, 'sector', None),
                datetime.now(),
                datetime.now()
            ) for rc in race_control_list]
            
            await conn.executemany('''
                INSERT INTO public.race_control_messages (
                    session_id, timestamp, utc_time, category, message,
                    flag, scope, sector, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir mensagens de controle na race_control_messages: {e}")
    
    async def _load_car_positions(self, conn, car_positions_list) -> None:
        """Carrega posições dos carros na tabela car_positions do Supabase"""
        if not car_positions_list:
            return
            
        try:
            values = [(
                1,  # session_id placeholder
                pos.timestamp,  # timestamp with time zone OK
                getattr(pos, 'utc_time', None),
                str(pos.driver_number),
                getattr(pos, 'x_coord', pos.x if hasattr(pos, 'x') else None),
                getattr(pos, 'y_coord', pos.y if hasattr(pos, 'y') else None),
                getattr(pos, 'z_coord', pos.z if hasattr(pos, 'z') else None),
                datetime.now(),
                datetime.now()
            ) for pos in car_positions_list]
            
            await conn.executemany('''
                INSERT INTO public.car_positions (
                    session_id, timestamp, utc_time, driver_number,
                    x_coord, y_coord, z_coord, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', values)
            
        except Exception as e:
            logger.error(f"Erro ao inserir posições dos carros na car_positions: {e}")
    
    async def _load_weather(self, conn, weather_list: List[Weather]) -> None:
        """Carrega dados meteorológicos no Supabase usando a tabela weather_data existente"""
        if not weather_list:
            return
            
        try:
            # Note: usando a estrutura da tabela weather_data existente
            values = [(
                None,  # session_id será None se não especificado
                w.timestamp.replace(tzinfo=None) if w.timestamp.tzinfo else w.timestamp,  # timestamp without time zone
                w.air_temp, w.track_temp, w.humidity,
                w.pressure, w.wind_speed, w.wind_direction, w.rainfall,
                datetime.now(), datetime.now()
            ) for w in weather_list]
            
            await conn.executemany('''
                INSERT INTO public.weather_data (
                    session_id, timestamp, air_temp, track_temp, humidity,
                    pressure, wind_speed, wind_direction, rainfall,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', values)
        except Exception as e:
            logger.error(f"Erro ao inserir weather em lote: {e}")