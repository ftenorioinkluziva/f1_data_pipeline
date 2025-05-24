#!/usr/bin/env python3
"""
Script para verificar se as tabelas necessárias existem no banco de dados Supabase
e se têm a estrutura esperada.
"""

import asyncio
import os
from typing import Dict, List
from dotenv import load_dotenv
import asyncpg
from loguru import logger

# Carrega variáveis de ambiente
load_dotenv()

# Configurações do banco de dados
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configurar logger para console apenas
logger.remove()
logger.add(lambda msg: print(msg), level="INFO")

class TableVerifier:
    """Verifica se as tabelas necessárias existem e têm a estrutura correta"""
    
    def __init__(self):
        self.conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        self.conn = None
    
    async def connect(self):
        """Conecta ao banco de dados"""
        try:
            self.conn = await asyncpg.connect(dsn=self.conn_string, ssl="require")
            logger.info("✅ Conexão com o banco de dados estabelecida")
            return True
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao banco de dados: {e}")
            return False
    
    async def close(self):
        """Fecha conexão com o banco de dados"""
        if self.conn:
            await self.conn.close()
            logger.info("Conexão fechada")
    
    async def verify_all_tables(self):
        """Verifica todas as tabelas necessárias"""
        if not await self.connect():
            return False
        
        try:
            logger.info("🔍 Verificando tabelas necessárias...")
            
            # Lista de tabelas necessárias (usando os nomes corretos do banco)
            required_tables = {
                'sessions': self.verify_sessions_table,
                'weather_data': self.verify_weather_data_table,
                'session_drivers': self.verify_session_drivers_table,
                'driver_positions': self.verify_generic_table,
                'car_positions': self.verify_generic_table,
                'car_telemetry': self.verify_car_telemetry_table,
                'race_control_messages': self.verify_race_control_messages_table,
                'team_radio': self.verify_generic_table,
            }
            
            all_ok = True
            
            for table_name, verify_func in required_tables.items():
                logger.info(f"\n📋 Verificando tabela: {table_name}")
                if await verify_func(table_name):
                    logger.info(f"✅ Tabela {table_name}: OK")
                else:
                    logger.error(f"❌ Tabela {table_name}: PROBLEMA")
                    all_ok = False
            
            return all_ok
            
        except Exception as e:
            logger.error(f"❌ Erro durante verificação: {e}")
            return False
        finally:
            await self.close()
    
    async def verify_generic_table(self, table_name: str) -> bool:
        """Verificação genérica se a tabela existe"""
        try:
            result = await self.conn.fetchrow('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            ''', table_name)
            
            if result:
                # Conta quantas colunas tem
                columns = await self.conn.fetch('''
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = $1
                ''', table_name)
                
                logger.info(f"   Colunas encontradas: {len(columns)}")
                return True
            else:
                logger.error(f"   Tabela {table_name} não encontrada")
                return False
                
        except Exception as e:
            logger.error(f"   Erro ao verificar tabela {table_name}: {e}")
            return False
    
    def _check_type_compatibility(self, actual_type: str, expected_type: str) -> bool:
        """Verifica se os tipos são compatíveis"""
        # Normaliza os tipos para comparação
        type_mappings = {
            'timestamp without time zone': 'timestamp',
            'timestamp with time zone': 'timestamp', 
            'character varying': 'text',
            'varchar': 'text',
            'bigserial': 'bigint',
            'serial': 'integer',
            'double precision': 'float'
        }
        
        actual = type_mappings.get(actual_type, actual_type)
        expected = type_mappings.get(expected_type, expected_type)
        
        # Compatibilidades especiais
        if actual == expected:
            return True
        if actual in ['integer', 'serial'] and expected == 'integer':
            return True
        if actual in ['bigint', 'bigserial'] and expected == 'bigint':
            return True
        if actual == 'character varying' and expected == 'text':
            return True
        if actual == 'text' and expected == 'character varying':
            return True
        
        return False
    
    async def verify_sessions_table(self, table_name: str) -> bool:
        """Verifica especificamente a tabela sessions"""
        try:
            # Verifica se a tabela existe
            result = await self.conn.fetchrow('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            ''', table_name)
            
            if not result:
                logger.error(f"   Tabela {table_name} não encontrada")
                return False
            
            # Verifica estrutura específica
            columns = await self.conn.fetch('''
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            ''', table_name)
            
            column_info = {row['column_name']: row['data_type'] for row in columns}
            
            # Campos essenciais esperados
            expected_fields = {
                'id': 'integer',
                'race_id': 'integer', 
                'key': 'integer',
                'type': 'text',
                'name': 'text'
            }
            
            missing_fields = []
            wrong_type_fields = []
            
            for field, expected_type in expected_fields.items():
                if field not in column_info:
                    missing_fields.append(field)
                elif not self._check_type_compatibility(column_info[field], expected_type):
                    wrong_type_fields.append(f"{field}: esperado {expected_type}, encontrado {column_info[field]}")
            
            if missing_fields:
                logger.error(f"   Campos obrigatórios não encontrados: {missing_fields}")
                return False
            
            if wrong_type_fields:
                logger.warning(f"   Campos com tipos diferentes: {wrong_type_fields}")
            
            logger.info(f"   Total de colunas: {len(columns)}")
            logger.debug(f"   Estrutura: {list(column_info.keys())}")
            
            # Testa se consegue fazer uma consulta simples
            count = await self.conn.fetchval('SELECT COUNT(*) FROM public.sessions')
            logger.info(f"   Registros na tabela: {count}")
            
            return True
            
        except Exception as e:
            logger.error(f"   Erro ao verificar tabela {table_name}: {e}")
            return False
    
    async def verify_weather_data_table(self, table_name: str) -> bool:
        """Verifica especificamente a tabela weather_data"""
        try:
            # Verifica se a tabela existe
            result = await self.conn.fetchrow('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            ''', table_name)
            
            if not result:
                logger.error(f"   Tabela {table_name} não encontrada")
                return False
            
            # Verifica estrutura específica
            columns = await self.conn.fetch('''
                SELECT column_name, data_type, numeric_precision, numeric_scale
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            ''', table_name)
            
            column_info = {row['column_name']: row['data_type'] for row in columns}
            
            # Campos essenciais esperados
            expected_fields = {
                'id': 'integer',
                'session_id': 'integer',
                'timestamp': 'timestamp',
                'air_temp': 'numeric',
                'track_temp': 'numeric',
                'humidity': 'numeric',
                'pressure': 'numeric'
            }
            
            missing_fields = []
            wrong_type_fields = []
            
            for field, expected_type in expected_fields.items():
                if field not in column_info:
                    missing_fields.append(field)
                elif not self._check_type_compatibility(column_info[field], expected_type):
                    wrong_type_fields.append(f"{field}: esperado {expected_type}, encontrado {column_info[field]}")
            
            if missing_fields:
                logger.error(f"   Campos obrigatórios não encontrados: {missing_fields}")
                return False
            
            if wrong_type_fields:
                logger.warning(f"   Campos com tipos diferentes: {wrong_type_fields}")
            
            logger.info(f"   Total de colunas: {len(columns)}")
            
            # Verifica foreign key para sessions
            try:
                fk_result = await self.conn.fetch('''
                    SELECT tc.table_name, kcu.column_name, 
                           ccu.table_name AS foreign_table_name,
                           ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_name = $1
                        AND kcu.column_name = 'session_id'
                ''', table_name)
                
                if fk_result:
                    logger.info(f"   Foreign key encontrada: session_id -> sessions(id)")
                else:
                    logger.warning(f"   Foreign key session_id -> sessions não encontrada")
                    
            except Exception as e:
                logger.warning(f"   Erro ao verificar foreign keys: {e}")
            
            # Testa se consegue fazer uma consulta simples
            count = await self.conn.fetchval('SELECT COUNT(*) FROM public.weather_data')
            logger.info(f"   Registros na tabela: {count}")
            
            return True
            
        except Exception as e:
            logger.error(f"   Erro ao verificar tabela {table_name}: {e}")
            return False
    
    async def verify_session_drivers_table(self, table_name: str) -> bool:
        """Verifica especificamente a tabela session_drivers"""
        try:
            # Verifica se a tabela existe
            result = await self.conn.fetchrow('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            ''', table_name)
            
            if not result:
                logger.error(f"   Tabela {table_name} não encontrada")
                return False
            
            # Verifica estrutura
            columns = await self.conn.fetch('''
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            ''', table_name)
            
            column_info = {row['column_name']: row['data_type'] for row in columns}
            
            # Campos essenciais esperados
            expected_fields = {
                'id': 'integer',
                'session_id': 'integer',
                'driver_number': 'character varying',
                'full_name': 'character varying',
                'team_name': 'character varying'
            }
            
            missing_fields = []
            for field, expected_type in expected_fields.items():
                if field not in column_info:
                    missing_fields.append(field)
                else:
                    logger.debug(f"   Campo {field}: {column_info[field]} (esperado: {expected_type})")
            
            if missing_fields:
                logger.error(f"   Campos obrigatórios não encontrados: {missing_fields}")
                return False
            
            logger.info(f"   Total de colunas: {len(columns)}")
            
            # Testa consulta simples
            count = await self.conn.fetchval('SELECT COUNT(*) FROM public.session_drivers')
            logger.info(f"   Registros na tabela: {count}")
            
            return True
            
        except Exception as e:
            logger.error(f"   Erro ao verificar tabela {table_name}: {e}")
            return False
    
    async def verify_car_telemetry_table(self, table_name: str) -> bool:
        """Verifica especificamente a tabela car_telemetry"""
        try:
            # Verifica se a tabela existe
            result = await self.conn.fetchrow('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            ''', table_name)
            
            if not result:
                logger.error(f"   Tabela {table_name} não encontrada")
                return False
            
            # Verifica estrutura
            columns = await self.conn.fetch('''
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            ''', table_name)
            
            column_info = {row['column_name']: row['data_type'] for row in columns}
            
            # Campos essenciais esperados
            expected_fields = {
                'id': 'integer',
                'session_id': 'integer',
                'driver_number': 'character varying',
                'timestamp': 'timestamp',
                'rpm': 'integer',
                'speed': 'integer'
            }
            
            missing_fields = []
            for field, expected_type in expected_fields.items():
                if field not in column_info:
                    missing_fields.append(field)
            
            if missing_fields:
                logger.error(f"   Campos obrigatórios não encontrados: {missing_fields}")
                return False
            
            logger.info(f"   Total de colunas: {len(columns)}")
            
            # Testa consulta simples
            count = await self.conn.fetchval('SELECT COUNT(*) FROM public.car_telemetry')
            logger.info(f"   Registros na tabela: {count}")
            
            return True
            
        except Exception as e:
            logger.error(f"   Erro ao verificar tabela {table_name}: {e}")
            return False
    
    async def verify_race_control_messages_table(self, table_name: str) -> bool:
        """Verifica especificamente a tabela race_control_messages"""
        try:
            # Verifica se a tabela existe
            result = await self.conn.fetchrow('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = $1
            ''', table_name)
            
            if not result:
                logger.error(f"   Tabela {table_name} não encontrada")
                return False
            
            # Verifica estrutura
            columns = await self.conn.fetch('''
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            ''', table_name)
            
            column_info = {row['column_name']: row['data_type'] for row in columns}
            
            # Campos essenciais esperados
            expected_fields = {
                'id': 'bigint',
                'session_id': 'bigint',
                'timestamp': 'timestamp',
                'message': 'text',
                'category': 'text'
            }
            
            missing_fields = []
            for field, expected_type in expected_fields.items():
                if field not in column_info:
                    missing_fields.append(field)
            
            if missing_fields:
                logger.error(f"   Campos obrigatórios não encontrados: {missing_fields}")
                return False
            
            logger.info(f"   Total de colunas: {len(columns)}")
            
            # Testa consulta simples
            count = await self.conn.fetchval('SELECT COUNT(*) FROM public.race_control_messages')
            logger.info(f"   Registros na tabela: {count}")
            
            return True
            
        except Exception as e:
            logger.error(f"   Erro ao verificar tabela {table_name}: {e}")
            return False
        """Verifica se os tipos são compatíveis"""
        # Normaliza os tipos para comparação
        type_mappings = {
            'timestamp without time zone': 'timestamp',
            'timestamp with time zone': 'timestamp',
            'character varying': 'text',
            'varchar': 'text'
        }
        
        actual = type_mappings.get(actual_type, actual_type)
        expected = type_mappings.get(expected_type, expected_type)
        
        return actual == expected or (actual in ['integer', 'serial'] and expected == 'integer')

async def main():
    """Função principal"""
    logger.info("🚀 Verificador de Tabelas do Pipeline F1")
    logger.info("=" * 50)
    
    # Verifica se as variáveis de ambiente estão configuradas
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        logger.error("❌ Variáveis de ambiente não configuradas corretamente")
        logger.error("Verifique se DB_HOST, DB_USER e DB_PASSWORD estão definidos no arquivo .env")
        return False
    
    logger.info(f"🔗 Conectando em: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    logger.info(f"👤 Usuário: {DB_USER}")
    
    verifier = TableVerifier()
    success = await verifier.verify_all_tables()
    
    logger.info("=" * 50)
    if success:
        logger.info("✅ TODAS AS TABELAS ESTÃO OK! Pipeline pode ser executado.")
    else:
        logger.error("❌ PROBLEMAS ENCONTRADOS! Corrija as tabelas antes de executar o pipeline.")
    
    return success

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n⏹️  Verificação cancelada pelo usuário")
        exit(1)
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}")
        exit(1)