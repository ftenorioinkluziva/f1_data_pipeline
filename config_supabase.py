import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.getenv("VITE_SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("VITE_SUPABASE_ANON_KEY")

# Extrair dados da URL do Supabase para configuração PostgreSQL
DB_HOST = SUPABASE_URL.replace("https://", "").replace(".supabase.co", ".db.supabase.co") if SUPABASE_URL else None
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # Deve ser configurado no .env

# Caminho para o arquivo de dados da F1
F1_DATA_FILE = os.getenv("F1_DATA_FILE", "f1_data.txt")

# Lista de tópicos F1 para monitorar
F1_TOPICS = [
    "TimingData", 
    "RaceControlMessages", 
    "DriverRaceInfo", 
    "TimingAppData", 
    "Position.z", 
    "WeatherData", 
    "DriverList", 
    "SessionInfo", 
    "TeamRadio", 
    "CarData.z", 
    "PitLaneTimeCollection"
]

# Timeout para coleta de dados (em segundos) - padrão 3 horas
F1_TIMEOUT = int(os.getenv("F1_TIMEOUT", "10800"))

# Intervalo para processar lotes de dados (em milissegundos)
BATCH_INTERVAL_MS = int(os.getenv("BATCH_INTERVAL_MS", "100"))  # Padrão: 100ms