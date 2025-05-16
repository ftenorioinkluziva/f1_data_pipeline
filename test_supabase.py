import asyncio
import sys
from dotenv import load_dotenv

from loguru import logger
from supabase_loader import SupabaseLoader

# Configura o logger
logger.remove()
logger.add(sys.stdout, level="INFO")

async def test_supabase_connection():
    """Testa a conexão com o Supabase"""
    
    logger.info("Iniciando teste de conexão com o Supabase...")
    
    # Carrega as variáveis de ambiente
    load_dotenv()
    
    # Inicializa o loader do Supabase
    loader = SupabaseLoader()
    
    try:
        # Tenta conectar ao Supabase
        logger.info("Conectando ao Supabase...")
        await loader.connect()
        logger.info("✅ Conexão com o Supabase estabelecida com sucesso!")
        
        # Testa a criação das tabelas
        logger.info("Verificando/criando tabelas...")
        await loader._ensure_tables_exist()
        logger.info("✅ Tabelas verificadas/criadas com sucesso!")
        
        # Lista as tabelas disponíveis
        async with loader.pool.acquire() as conn:
            tables = await conn.fetch('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            ''')
            
            logger.info(f"Tabelas disponíveis no Supabase ({len(tables)}):")
            for table in tables:
                logger.info(f"  - {table['table_name']}")
        
        logger.info("Teste de conexão concluído com sucesso!")
    
    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao Supabase: {e}")
        logger.error("Verifique suas credenciais no arquivo .env")
    
    finally:
        # Desconecta do Supabase
        if hasattr(loader, 'pool') and loader.pool:
            logger.info("Fechando conexão...")
            await loader.disconnect()
            logger.info("Conexão fechada")

if __name__ == "__main__":
    asyncio.run(test_supabase_connection())