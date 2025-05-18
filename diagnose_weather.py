import asyncio
import os
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
import asyncpg
from loguru import logger

# Configura o logger
logger.remove()
logger.add("debug_weather_data.log", rotation="10 MB", level="DEBUG")
logger.add(lambda msg: print(msg), level="INFO")

async def diagnose_weather_data_issues(session_id=None, input_file=None):
    """Script de diagnóstico para problemas com dados meteorológicos"""
    logger.info("=== INICIANDO DIAGNÓSTICO DE DADOS METEOROLÓGICOS ===")
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Verifica variáveis de ambiente
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    logger.info(f"Verificando configurações de banco de dados:")
    logger.info(f"Host: {db_host}")
    logger.info(f"Porta: {db_port}")
    logger.info(f"Banco: {db_name}")
    logger.info(f"Usuário: {db_user}")
    logger.info(f"Senha: {'*' * len(db_password) if db_password else 'NÃO DEFINIDA!'}")
    
    conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Tenta conectar ao banco
    logger.info("\n=== TESTE DE CONEXÃO ===")
    try:
        conn = await asyncpg.connect(
            dsn=conn_string,
            timeout=10,
            ssl="require"
        )
        logger.info("✅ Conexão com o banco estabelecida com sucesso!")
        
        # Verifica versão do PostgreSQL
        version = await conn.fetchval("SELECT version()")
        logger.info(f"PostgreSQL: {version}")
        
        # FASE 1: Verificação das tabelas
        logger.info("\n=== VERIFICAÇÃO DE TABELAS ===")
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        logger.info(f"Tabelas encontradas ({len(tables)}):")
        for table in tables:
            logger.info(f"  - {table['table_name']}")
        
        # Verifica se a tabela weather_data existe
        weather_table_exists = any(t['table_name'] == 'weather_data' for t in tables)
        if weather_table_exists:
            logger.info("✅ Tabela weather_data encontrada!")
        else:
            logger.error("❌ Tabela weather_data NÃO encontrada!")
            logger.info("Criando tabela weather_data para teste...")
            try:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS public.weather_data (
                        id serial not null,
                        session_id integer null,
                        timestamp timestamp without time zone not null,
                        air_temp numeric(5, 2) null,
                        track_temp numeric(5, 2) null,
                        humidity numeric(5, 2) null,
                        pressure numeric(6, 2) null,
                        rainfall numeric(5, 2) null,
                        wind_direction integer null,
                        wind_speed numeric(5, 2) null,
                        created_at timestamp without time zone null default now(),
                        updated_at timestamp without time zone null default now(),
                        constraint weather_data_pkey primary key (id)
                    )
                """)
                logger.info("✅ Tabela weather_data criada com sucesso!")
            except Exception as e:
                logger.error(f"❌ Erro ao criar tabela: {e}")
        
        # FASE 2: Verificação da tabela de sessões
        logger.info("\n=== VERIFICAÇÃO DA TABELA DE SESSÕES ===")
        
        # Verifica se a tabela sessions existe
        sessions_table_exists = any(t['table_name'] == 'sessions' for t in tables)
        if sessions_table_exists:
            logger.info("✅ Tabela sessions encontrada!")
            
            # Verifica se há sessões cadastradas
            sessions = await conn.fetch("""
                SELECT id, session_id, name, date, circuit 
                FROM public.sessions 
                ORDER BY date DESC
                LIMIT 10
            """)
            
            logger.info(f"Sessões encontradas ({len(sessions)}):")
            for session in sessions:
                logger.info(f"  - ID: {session['id']}, Key: {session['session_key']}, Nome: {session['name']}, Data: {session['date']}, Circuito: {session['circuit']}")
            
            # Verifica se a session_id especificada existe
            if session_id:
                session_exists = await conn.fetchrow("""
                    SELECT id, session_key, name 
                    FROM public.sessions 
                    WHERE id = $1
                """, session_id)
                
                if session_exists:
                    logger.info(f"✅ Sessão ID={session_id} encontrada: {session_exists['name']}")
                else:
                    logger.warning(f"⚠️ Sessão ID={session_id} NÃO encontrada!")
        else:
            logger.warning("⚠️ Tabela sessions NÃO encontrada! Isso pode causar problemas com a chave estrangeira.")
        
        # FASE 3: Verificação de dados meteorológicos
        logger.info("\n=== VERIFICAÇÃO DE DADOS METEOROLÓGICOS ===")
        
        if weather_table_exists:
            # Conta registros
            count = await conn.fetchval("SELECT COUNT(*) FROM public.weather_data")
            logger.info(f"Total de registros na tabela weather_data: {count}")
            
            # Verifica registros por sessão
            if session_id:
                session_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM public.weather_data 
                    WHERE session_id = $1
                """, session_id)
                
                logger.info(f"Registros para a sessão ID={session_id}: {session_count}")
            
            # Amostra de dados
            sample = await conn.fetch("""
                SELECT id, session_id, timestamp, air_temp, track_temp, wind_speed 
                FROM public.weather_data 
                ORDER BY id DESC 
                LIMIT 5
            """)
            
            if sample:
                logger.info("Últimos 5 registros:")
                for row in sample:
                    logger.info(f"  ID: {row['id']}, Sessão: {row['session_id']}, Temp: {row['air_temp']}°C, Pista: {row['track_temp']}°C")
            else:
                logger.info("Nenhum registro encontrado na tabela.")
        
        # FASE 4: Teste de inserção
        logger.info("\n=== TESTE DE INSERÇÃO ===")
        try:
            # Tenta inserir um registro de teste
            test_session_id = session_id or 1
            await conn.execute("""
                INSERT INTO public.weather_data (
                    session_id, timestamp, air_temp, track_temp, humidity,
                    pressure, rainfall, wind_direction, wind_speed,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """, 
                test_session_id, datetime.now(), 25.5, 30.2, 60.0,
                1013.25, 0.0, 180, 10.5,
                datetime.now(), datetime.now()
            )
            logger.info("✅ Inserção de teste realizada com sucesso!")
            
            # Verifica se o registro foi inserido
            inserted = await conn.fetchval("""
                SELECT COUNT(*) FROM public.weather_data 
                WHERE air_temp = 25.5 AND track_temp = 30.2
            """)
            
            logger.info(f"Registros encontrados com os valores de teste: {inserted}")
        except Exception as e:
            logger.error(f"❌ Erro ao inserir registro de teste: {e}")
        
        # FASE 5: Verificação de dados do arquivo
        if input_file and os.path.exists(input_file):
            logger.info(f"\n=== ANÁLISE DO ARQUIVO DE DADOS {input_file} ===")
            
            try:
                file_size = os.path.getsize(input_file)
                logger.info(f"Tamanho do arquivo: {file_size/1024:.2f} KB")
                
                # Conta linhas no arquivo
                with open(input_file, 'r') as f:
                    lines = f.readlines()
                    
                logger.info(f"Total de linhas no arquivo: {len(lines)}")
                
                # Conta linhas de dados meteorológicos
                weather_lines = []
                for i, line in enumerate(lines):
                    try:
                        data = json.loads(line)
                        if data.get('topic') == 'WeatherData':
                            weather_lines.append((i+1, data))
                    except:
                        pass
                
                logger.info(f"Total de linhas com dados meteorológicos: {len(weather_lines)}")
                
                # Mostra os primeiros 3 registros de dados meteorológicos
                if weather_lines:
                    logger.info("Primeiros registros de dados meteorológicos encontrados:")
                    for i, (line_num, data) in enumerate(weather_lines[:3]):
                        logger.info(f"Linha {line_num}:")
                        logger.info(f"  Timestamp: {data.get('timestamp')}")
                        logger.info(f"  Dados: {data.get('data')}")
                else:
                    logger.warning("⚠️ Nenhum dado meteorológico encontrado no arquivo!")
            except Exception as e:
                logger.error(f"❌ Erro ao analisar arquivo: {e}")
        
        # FASE 6: Verifica permissões do usuário
        logger.info("\n=== VERIFICAÇÃO DE PERMISSÕES ===")
        try:
            permissions = await conn.fetch("""
                SELECT table_name, privilege_type
                FROM information_schema.table_privileges
                WHERE grantee = current_user
                  AND table_schema = 'public'
                ORDER BY table_name, privilege_type
            """)
            
            logger.info(f"Permissões do usuário atual ({db_user}):")
            current_table = None
            table_perms = []
            
            for perm in permissions:
                if current_table != perm['table_name']:
                    if current_table:
                        logger.info(f"  - {current_table}: {', '.join(table_perms)}")
                    current_table = perm['table_name']
                    table_perms = []
                
                table_perms.append(perm['privilege_type'])
            
            if current_table:
                logger.info(f"  - {current_table}: {', '.join(table_perms)}")
            
            # Verifica especificamente permissões na tabela weather_data
            weather_perms = [p['privilege_type'] for p in permissions if p['table_name'] == 'weather_data']
            if 'INSERT' in weather_perms:
                logger.info("✅ Usuário tem permissão INSERT na tabela weather_data")
            else:
                logger.warning("⚠️ Usuário NÃO tem permissão INSERT na tabela weather_data!")
        except Exception as e:
            logger.error(f"❌ Erro ao verificar permissões: {e}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        return
    
    finally:
        if 'conn' in locals() and conn:
            await conn.close()
            logger.info("Conexão fechada.")
    
    logger.info("\n=== DIAGNÓSTICO CONCLUÍDO ===")
    logger.info("Verifique o arquivo debug_weather_data.log para detalhes adicionais.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Diagnostica problemas com dados meteorológicos')
    parser.add_argument('--session-id', type=int, help='ID da sessão a ser verificada')
    parser.add_argument('--input-file', type=str, help='Arquivo de dados para análise')
    
    args = parser.parse_args()
    
    asyncio.run(diagnose_weather_data_issues(args.session_id, args.input_file))