import asyncio
import os
import signal
import time
import traceback
import ast  # Para avaliar expressões Python com segurança
import argparse
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv

from loguru import logger
import asyncpg

# Configurar o logger
logger.remove()
logger.add("f1_weather_extractor.log", rotation="10 MB", level="DEBUG")
logger.add(lambda msg: print(msg), level="INFO")

# Flag para controlar o encerramento
shutdown_requested = False

def handle_shutdown(signum, frame):
    """Manipula solicitações de encerramento gracioso"""
    global shutdown_requested
    logger.info(f"Sinal de encerramento recebido ({signum})")
    shutdown_requested = True

class WeatherDataProcessor:
    """Processa dados meteorológicos e insere no banco de dados"""
    
    def __init__(self, session_id: int, conn_string: str):
        self.session_id = session_id
        self.conn_string = conn_string
        self.conn = None
        self.processed_count = 0
        self.connected = False
    
    async def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            self.conn = await asyncpg.connect(
                dsn=self.conn_string,
                ssl="require"
            )
            self.connected = True
            logger.info(f"Conexão com o banco de dados estabelecida para session_id={self.session_id}")
            
            # Verifica se a sessão existe (usando campos corretos da tabela sessions)
            try:
                session = await self.conn.fetchrow(
                    "SELECT id, key, name, type FROM public.sessions WHERE id = $1",
                    self.session_id
                )
                
                if session:
                    logger.info(f"Sessão encontrada: ID={session['id']}, Key={session['key']}, Name={session['name']}, Type={session['type']}")
                else:
                    logger.warning(f"Sessão ID={self.session_id} não encontrada! Os dados serão inseridos mesmo assim.")
            except Exception as e:
                logger.error(f"Erro ao verificar sessão: {e}")
                logger.info("Continuando sem verificar sessão...")
                
            # Verifica a estrutura da tabela weather_data
            try:
                weather_columns = await self.conn.fetch("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'weather_data'
                    ORDER BY ordinal_position
                """)
                
                logger.debug(f"Estrutura da tabela weather_data:")
                for col in weather_columns:
                    logger.debug(f"  - {col['column_name']} ({col['data_type']})")
            except Exception as e:
                logger.error(f"Erro ao verificar estrutura da tabela weather_data: {e}")
        
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
    
    async def process_weather_data(self, topic: str, data: Dict, timestamp_str: str) -> bool:
        """Processa dados meteorológicos do formato específico"""
        if not self.connected:
            await self.connect()
        
        try:
            # Verificar se é dados meteorológicos
            if topic != 'WeatherData':
                return False
            
            # Parse do timestamp - IMPORTANTE: usar timestamp without time zone 
            if timestamp_str:
                try:
                    # Parse como timezone-aware e depois converte para naive
                    aware_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    timestamp = aware_dt.replace(tzinfo=None)  # Remove timezone info
                except ValueError:
                    timestamp = datetime.now()
            else:
                timestamp = datetime.now()
            
            # Log dos dados brutos recebidos
            logger.debug(f"Dados meteorológicos brutos: {data}")
            
            # Valores convertidos para os tipos corretos da tabela weather_data
            fields = {
                'air_temp': self._parse_numeric(data.get('AirTemp', '')),
                'track_temp': self._parse_numeric(data.get('TrackTemp', '')),
                'humidity': self._parse_numeric(data.get('Humidity', '')),
                'pressure': self._parse_numeric(data.get('Pressure', '')),
                'rainfall': self._parse_numeric(data.get('Rainfall', '0')),  # numeric, não boolean
                'wind_direction': self._parse_int(data.get('WindDirection', '')),
                'wind_speed': self._parse_numeric(data.get('WindSpeed', ''))
            }
            
            # Log detalhado do que estamos tentando inserir
            logger.debug(f"Inserindo dados meteorológicos: {timestamp}, Temp: {fields['air_temp']}°C, Pista: {fields['track_temp']}°C")
            
            # Timestamps de criação/atualização como naive (without time zone)
            now = datetime.now()
            
            # Inserir no banco de dados usando a estrutura correta da tabela weather_data
            await self.conn.execute("""
                INSERT INTO public.weather_data (
                    session_id, timestamp, air_temp, track_temp, humidity,
                    pressure, rainfall, wind_direction, wind_speed,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """,
                self.session_id, timestamp, 
                fields['air_temp'], fields['track_temp'], fields['humidity'],
                fields['pressure'], fields['rainfall'], fields['wind_direction'], fields['wind_speed'],
                now, now
            )
            
            self.processed_count += 1
            if self.processed_count % 10 == 0:
                logger.info(f"Processados {self.processed_count} registros meteorológicos até agora")
            
            return True
        
        except Exception as e:
            logger.error(f"Erro ao processar dados meteorológicos: {e}")
            logger.debug(f"Dados que causaram o erro: {data}")
            return False
    
    def _parse_numeric(self, value: Any) -> Optional[float]:
        """Converte valor para numeric (float) ou retorna None"""
        if value is None or value == '':
            return None
        
        # Se for boolean ou string boolean, converte para número
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, str) and value.lower() in ['true', 'false']:
            return 1.0 if value.lower() == 'true' else 0.0
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_int(self, value: Any) -> Optional[int]:
        """Converte valor para inteiro ou retorna None"""
        if value is None or value == '':
            return None
        
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    async def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.conn:
            await self.conn.close()
            logger.info("Conexão com o banco de dados fechada")

def parse_data_line(line: str) -> Tuple[str, Dict, str]:
    """Analisa uma linha de dados no formato específico"""
    try:
        # Use ast.literal_eval para processar seguramente a lista Python
        parsed_data = ast.literal_eval(line)
        
        # Formato esperado: [tópico, dados, timestamp]
        if isinstance(parsed_data, list) and len(parsed_data) >= 3:
            topic = parsed_data[0]
            data = parsed_data[1]
            timestamp = parsed_data[2]
            return topic, data, timestamp
        
        raise ValueError("Formato de dados inesperado")
    
    except Exception as e:
        raise ValueError(f"Erro ao analisar linha: {e}")

async def monitor_weather_data(input_file: str, session_id: int):
    """Monitora um arquivo de dados F1 para dados meteorológicos"""
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Configurações do banco de dados
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    conn_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Registra manipuladores de sinais para encerramento gracioso
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    logger.info(f"Iniciando monitoramento de dados meteorológicos para a sessão ID={session_id}")
    logger.info(f"Arquivo de entrada: {input_file}")
    
    # Inicializa o processador de dados meteorológicos
    processor = WeatherDataProcessor(session_id=session_id, conn_string=conn_string)
    
    try:
        # Conecta ao banco de dados
        await processor.connect()
        
        # Verifica se o arquivo existe
        if not os.path.exists(input_file):
            logger.warning(f"Arquivo {input_file} não encontrado. Aguardando sua criação...")
        
        # Posição da última leitura
        last_position = 0
        
        # Estatísticas
        start_time = time.time()
        last_report_time = start_time
        total_lines = 0
        weather_data_found = 0
        
        # Loop principal de monitoramento
        while not shutdown_requested:
            try:
                # Verifica se o arquivo existe
                if not os.path.exists(input_file):
                    await asyncio.sleep(1)
                    continue
                
                # Obtém tamanho atual do arquivo
                file_size = os.path.getsize(input_file)
                
                # Se há novos dados
                if file_size > last_position:
                    with open(input_file, 'r') as f:
                        # Move para a última posição lida
                        f.seek(last_position)
                        
                        # Lê as novas linhas
                        lines = f.readlines()
                        last_position = f.tell()
                        
                        total_lines += len(lines)
                        
                        # Processa as linhas
                        for line in lines:
                            try:
                                # Analisa a linha no formato específico
                                topic, data, timestamp = parse_data_line(line)
                                
                                # Se for dados meteorológicos, processa
                                if topic == 'WeatherData':
                                    success = await processor.process_weather_data(topic, data, timestamp)
                                    if success:
                                        weather_data_found += 1
                            except ValueError as e:
                                # Ignora erros de linhas malformadas
                                pass
                            except Exception as e:
                                logger.error(f"Erro ao processar linha: {e}")
                
                # Relatório periódico
                current_time = time.time()
                if current_time - last_report_time >= 60:  # a cada minuto
                    elapsed = current_time - start_time
                    logger.info(f"=== Relatório de Progresso ===")
                    logger.info(f"Tempo em execução: {elapsed:.1f}s")
                    logger.info(f"Linhas processadas: {total_lines}")
                    logger.info(f"Registros meteorológicos encontrados: {weather_data_found}")
                    logger.info(f"Registros meteorológicos inseridos: {processor.processed_count}")
                    
                    if file_size > 0:
                        logger.info(f"Tamanho do arquivo: {file_size/1024:.1f} KB")
                        logger.info(f"Posição atual: {last_position/1024:.1f} KB ({last_position/file_size*100:.1f}%)")
                    
                    last_report_time = current_time
                
                # Pequena pausa
                await asyncio.sleep(0.5)
                
            except asyncio.CancelledError:
                logger.info("Monitoramento cancelado")
                break
            except Exception as e:
                logger.error(f"Erro no loop de monitoramento: {e}")
                logger.debug(traceback.format_exc())
                await asyncio.sleep(1)  # Espera um pouco mais em caso de erro
        
        # Relatório final
        elapsed = time.time() - start_time
        logger.info(f"=== Relatório Final ===")
        logger.info(f"Tempo total: {elapsed:.1f}s")
        logger.info(f"Linhas processadas: {total_lines}")
        logger.info(f"Registros meteorológicos encontrados: {weather_data_found}")
        logger.info(f"Registros meteorológicos inseridos: {processor.processed_count}")
    
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        logger.debug(traceback.format_exc())
    
    finally:
        # Fecha a conexão
        await processor.close()
        logger.info("Monitoramento encerrado")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Monitora dados meteorológicos da F1')
    parser.add_argument('--session-id', type=int, required=True, help='ID da sessão')
    parser.add_argument('--input-file', type=str, default='f1_data.txt', help='Arquivo de entrada')
    
    args = parser.parse_args()
    
    try:
        asyncio.run(monitor_weather_data(args.input_file, args.session_id))
    except KeyboardInterrupt:
        print("\nPrograma encerrado pelo usuário")