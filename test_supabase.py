import os
import sys
from dotenv import load_dotenv
import psycopg2
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stdout, level="INFO")

def test_direct_connection():
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    logger.info(f"Testing direct connection to {db_host}:{db_port} as {db_user}...")
    
    try:
        # Connect using psycopg2
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            sslmode='require'
        )
        
        logger.info("✅ Connected successfully!")
        
        # Test query
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            logger.info(f"PostgreSQL version: {version}")
            
            # List tables
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = cur.fetchall()
            
            logger.info(f"Found {len(tables)} tables in database:")
            for table in tables:
                logger.info(f"  - {table[0]}")
        
        # Close connection
        conn.close()
        logger.info("Connection closed.")
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        
        # Special handling for common errors
        error_str = str(e)
        if "password authentication failed" in error_str:
            logger.error("Password authentication failed. Check your DB_PASSWORD.")
        elif "could not translate host name" in error_str:
            logger.error("Could not resolve hostname. Check your DB_HOST.")
        elif "connection refused" in error_str:
            logger.error("Connection refused. Check if the port is open and accessible.")

if __name__ == "__main__":
    test_direct_connection()