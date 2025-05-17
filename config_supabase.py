import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração do Supabase
SUPABASE_URL = os.getenv("VITE_SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("VITE_SUPABASE_ANON_KEY")

# Configuração para conexão PostgreSQL direta
# Ao invés de calcular o host, utilizamos diretamente o valor do .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "6543")  # Atualizado para o novo valor padrão
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

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