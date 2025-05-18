import asyncio
import os
from datetime import datetime
from dotenv import load_dotenv
import asyncpg
from loguru import logger

# Configura o logger
logger.remove()
logger.add(lambda msg: print(msg), level="INFO")

async def test_weather_table(session_id=1):
    """Teste da tabela weather_data"""
    logger.info("=== TESTE DA TABELA WEATHER_DATA ===")
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Obtém configurações do banco
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        # Conecta ao banco
        conn = await asyncpg.connect(
            dsn=conn_string,
            ssl="require"
        )
        logger.info("Conexão estabelecida")
        
        # Verifica a estrutura da tabela
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'weather_data'
            ORDER BY ordinal_position
        """)
        
        if not columns:
            logger.error("❌ Tabela weather_data não encontrada!")
            return
        
        logger.info("Estrutura da tabela weather_data:")
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            logger.info(f"  - {col['column_name']} ({col['data_type']}) {nullable}")
        
        # Insere um registro de teste
        now = datetime.now()
        logger.info(f"Inserindo registro de teste com session_id = {session_id}")
        
        try:
            await conn.execute("""
                INSERT INTO public.weather_data (
                    session_id, timestamp, air_temp, track_temp, humidity,
                    pressure, rainfall, wind_direction, wind_speed,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
                session_id, now, 26.5, 35.8, 45.0,
                1012.5, 0.0, 90, 5.5,
                now, now
            )
            logger.info("✅ Registro inserido com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro ao inserir: {e}")
        
        # Verifica os registros
        records = await conn.fetch("""
            SELECT * FROM public.weather_data
            ORDER BY id DESC
            LIMIT 5
        """)
        
        logger.info(f"Últimos registros ({len(records)}):")
        for record in records:
            logger.info(f"  - ID: {record['id']}, Session: {record['session_id']}, " +
                      f"Air: {record['air_temp']}°C, Track: {record['track_temp']}°C, " +
                      f"Timestamp: {record['timestamp']}")
    
    except Exception as e:
        logger.error(f"Erro: {e}")
    
    finally:
        if 'conn' in locals() and conn:
            await conn.close()
            logger.info("Conexão fechada")

if __name__ == "__main__":
    import sys
    session_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    asyncio.run(test_weather_table(session_id))