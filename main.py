import asyncio
import os
import signal
import time
import traceback
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

from loguru import logger

from config_supabase import F1_DATA_FILE, BATCH_INTERVAL_MS, F1_TOPICS
from extractor import F1DataExtractor
from supabase_loader import SupabaseLoader

# Configura o parser de argumentos da linha de comando
def parse_args():
    parser = argparse.ArgumentParser(description='F1 Data Extractor com processamento de dados meteorológicos')
    parser.add_argument('--session-id', type=int, required=True, 
                        help='ID da sessão no banco de dados (obrigatório)')
    parser.add_argument('--output-file', type=str, default=F1_DATA_FILE,
                        help=f'Caminho para o arquivo de saída (padrão: {F1_DATA_FILE})')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Modo verboso - exibe mais logs de debug')
    
    return parser.parse_args()

class PerformanceMonitor:
    """Monitora a performance do pipeline"""
    
    def __init__(self):
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.total_lines_processed = 0
        self.file_size = 0
        self.weather_data_count = 0
    
    def record_file_size(self, file_size: int):
        """Registra o tamanho atual do arquivo"""
        self.file_size = file_size
    
    def record_weather_data(self, count: int):
        """Registra quantidade de dados meteorológicos processados"""
        self.weather_data_count += count
    
def report_if_needed(self, force: bool = False):
    """Gera relatório periódico de performance"""
    current_time = time.time()
    if force or (current_time - self.last_report_time >= 60):  # Relatório a cada minuto
        elapsed = current_time - self.start_time
        recent_batches = self.batch_times[-100:] if self.batch_times else []
        
        logger.info(f"=== Relatório de Performance ===")
        logger.info(f"Tempo total em execução: {elapsed:.2f}s")
        logger.info(f"Total de linhas processadas: {self.total_lines_processed}")
        logger.info(f"Total de registros inseridos: {self.total_records_inserted}")
        
        if recent_batches and sum(recent_batches) > 0:  # Verificação para evitar divisão por zero
            avg_batch_time = sum(recent_batches) / len(recent_batches)
            max_batch_time = max(recent_batches)
            logger.info(f"Tempo médio de processamento por lote: {avg_batch_time*1000:.2f}ms")
            logger.info(f"Tempo máximo de processamento por lote: {max_batch_time*1000:.2f}ms")
            logger.info(f"Taxa de processamento: {len(recent_batches)/sum(recent_batches):.2f} lotes/s")
        else:
            logger.info("Não há dados de processamento ou todos os tempos são zero")
        
        self.last_report_time = current_time

# Configura o logger
logger.add("f1_extraction.log", rotation="10 MB", level="INFO", retention="1 week")

# Flag para controlar o encerramento
shutdown_requested = False

def handle_shutdown(signum, frame):
    """Manipula solicitações de encerramento gracioso"""
    global shutdown_requested
    logger.info(f"Sinal de encerramento recebido ({signum})")
    shutdown_requested = True

class WeatherDataProcessor:
    """Processa e armazena dados meteorológicos no banco de dados"""
    
    def __init__(self, session_id: int):
        self.supabase = None
        self.session_id = session_id
        self.initialized = False
        self.session_info = None
    
    async def initialize(self):
        """Inicializa conexão com o banco de dados"""
        if self.initialized:
            return
        
        try:
            self.supabase = SupabaseLoader()
            await self.supabase.connect()
            self.initialized = True
            logger.info("Conexão com o banco de dados estabelecida para dados meteorológicos")
            
            # Verifica se a sessão especificada existe
            await self._check_session()
        except Exception as e:
            logger.error(f"Erro ao inicializar processador de dados meteorológicos: {e}")
            raise
    
    async def _check_session(self):
        """Verifica se a sessão especificada existe no banco de dados"""
        if not self.supabase or not self.supabase.pool:
            return
        
        try:
            async with self.supabase.pool.acquire() as conn:
                # Busca a sessão pelo ID
                row = await conn.fetchrow('''
                    SELECT id, key, name, start_date, end_date, race_id 
                    FROM public.sessions 
                    WHERE id = $1
                ''', self.key)
                
                if row:
                    self.session_info = dict(row)
                    logger.info(f"Sessão encontrada: ID={self.key},  Nome={row['name']}")
                else:
                    logger.warning(f"ATENÇÃO: Sessão com ID={self.key} não encontrada no banco de dados.")
                    logger.warning("Os dados serão inseridos com este ID mesmo assim, mas verifique se está correto!")
        except Exception as e:
            logger.error(f"Erro ao verificar sessão: {e}")
    
    async def process_weather_data(self, data: Dict):
        """Processa dados meteorológicos e insere no banco de dados"""
        if not self.initialized:
            await self.initialize()
        
        if not data.get('topic') == 'WeatherData' or 'data' not in data:
            return 0
        
        weather_data = data['data']
        timestamp_str = data.get('timestamp', '')
        
        try:
            # Parse do timestamp
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except ValueError:
                    timestamp = datetime.now()
            else:
                timestamp = datetime.now()
            
            # Parse dos dados meteorológicos
            air_temp = self._parse_float(weather_data.get('AirTemp', ''))
            track_temp = self._parse_float(weather_data.get('TrackTemp', ''))
            humidity = self._parse_float(weather_data.get('Humidity', ''))
            pressure = self._parse_float(weather_data.get('Pressure', ''))
            wind_speed = self._parse_float(weather_data.get('WindSpeed', ''))
            wind_direction = self._parse_int(weather_data.get('WindDirection', ''))
            rainfall = self._parse_float(weather_data.get('Rainfall', 'false'))
            
            # Inserção no banco de dados
            if self.supabase and self.supabase.pool:
                async with self.supabase.pool.acquire() as conn:
                    await conn.execute('''
                        INSERT INTO public.weather_data (
                            session_id, timestamp, air_temp, track_temp, humidity,
                            pressure, rainfall, wind_direction, wind_speed,
                            created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ''', 
                        self.id, timestamp, air_temp, track_temp, humidity,
                        pressure, rainfall, wind_direction, wind_speed,
                        datetime.now(), datetime.now()
                    )
                
                logger.debug(f"Dados meteorológicos inseridos: {timestamp}, Temp: {air_temp}°C, Pista: {track_temp}°C")
                
                query_result = await conn.fetchval('''
                    SELECT COUNT(*) FROM public.weather_data 
                    WHERE session_id = $1
                ''', self.session_id)

                logger.info(f"Total de registros meteorológicos para sessão {self.session_id}: {query_result}")
                return 1
                
        except Exception as e:
            logger.error(f"Erro ao processar dados meteorológicos: {e}")
        
        return 0
    
    def _parse_float(self, value: Any) -> Optional[float]:
        """Converte valor para float ou retorna None"""
        if value is None or value == '':
            return None
        
        if isinstance(value, bool) or value == 'true' or value == 'false':
            return 1.0 if str(value).lower() == 'true' else 0.0
        
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
        if self.supabase:
            await self.supabase.disconnect()
            logger.info("Conexão com o banco de dados fechada (processador de dados meteorológicos)")

async def main():
    """Função principal que executa a extração de dados da F1 e processa dados meteorológicos"""
    
    # Processa argumentos da linha de comando
    args = parse_args()
    
    # Configura o nível de log baseado no modo verboso
    if args.verbose:
        logger.level("DEBUG")
    
    # Usa o valor do arquivo de saída dos argumentos
    output_file = args.output_file
    
    # Registra manipuladores de sinais para encerramento gracioso
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    logger.info(f"Iniciando extração de dados F1 para o arquivo: {output_file}")
    logger.info(f"Tópicos monitorados: {', '.join(F1_TOPICS)}")
    logger.info(f"Processamento especial: dados meteorológicos serão inseridos no banco de dados")
    logger.info(f"ID da sessão especificado: {args.session_id}")
    
    # Inicializa o monitor de performance
    perf_monitor = PerformanceMonitor()
    
    # Inicializa o processador de dados meteorológicos com o session_id passado
    weather_processor = WeatherDataProcessor(session_id=args.session_id)
    
    try:
        # Inicializa o processador
        await weather_processor.initialize()
        
        # Inicializa apenas o extrator
        extractor = F1DataExtractor(output_file=output_file)
        
        # Inicia a extração em segundo plano
        logger.info(f"Iniciando extração de dados da F1 para arquivo: {output_file}")
        extraction_task = asyncio.create_task(extractor.start_extraction())
        
        # Contador de operações para o coração do loop
        heartbeat_counter = 0
        
        logger.info("Monitorando arquivo de dados...")
        
        # Posição da última leitura do arquivo
        last_position = 0
        
        # Loop principal - monitoramento e processamento de dados meteorológicos
        while not shutdown_requested:
            try:
                # Monitora o arquivo
                if os.path.exists(output_file):
                    file_size = os.path.getsize(output_file)
                    perf_monitor.record_file_size(file_size)
                    
                    # Verifica se há novos dados para ler
                    if file_size > last_position:
                        with open(output_file, 'r') as f:
                            # Move para a última posição lida
                            f.seek(last_position)
                            
                            # Lê as novas linhas
                            new_lines = f.readlines()
                            last_position = f.tell()
                            
                            # Processa apenas linhas com dados meteorológicos
                            weather_data_count = 0
                            for line in new_lines:
                                try:
                                    data = json.loads(line)
                                    if data.get('topic') == 'WeatherData':
                                        count = await weather_processor.process_weather_data(data)
                                        weather_data_count += count
                                except json.JSONDecodeError:
                                    pass
                                except Exception as e:
                                    logger.error(f"Erro ao processar linha: {e}")
                            
                            if weather_data_count > 0:
                                logger.info(f"Processados {weather_data_count} novos registros de dados meteorológicos")
                                perf_monitor.record_weather_data(weather_data_count)
                
                # Mostra sinal de vida periodicamente
                heartbeat_counter += 1
                if heartbeat_counter >= 300:  # A cada 5 minutos (com intervalo de 1s)
                    if os.path.exists(output_file):
                        file_size = os.path.getsize(output_file)
                        logger.info(f"Extração ativa, tamanho atual do arquivo: {file_size/1024:.2f} KB")
                    else:
                        logger.info("Extração ativa, aguardando criação do arquivo...")
                    heartbeat_counter = 0
                
                # Gera relatório de performance periódico
                perf_monitor.report_if_needed()
                
                # Pequena pausa
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("Loop de monitoramento cancelado externamente")
                break
            except Exception as e:
                logger.error(f"Erro no loop de monitoramento: {e}")
                # Espera um pouco mais em caso de erro
                await asyncio.sleep(1)
        
        # Encerramento
        logger.info("Encerrando extração...")
        
        # Encerra a extração
        logger.info("Interrompendo extração de dados...")
        extractor.stop_extraction()
        
        try:
            # Aguarda a conclusão da tarefa de extração
            await asyncio.wait_for(extraction_task, timeout=10.0)
            logger.info("Tarefa de extração concluída com sucesso")
        except asyncio.TimeoutError:
            logger.warning("Timeout ao aguardar conclusão da extração, forçando cancelamento")
            extraction_task.cancel()
        except asyncio.CancelledError:
            logger.info("Tarefa de extração cancelada")
        except Exception as e:
            logger.error(f"Erro ao encerrar tarefa de extração: {e}")
        
        # Fecha conexão com o banco de dados
        await weather_processor.close()
        
        # Relatório final
        logger.info("Gerando relatório final...")
        perf_monitor.report_if_needed(force=True)
        
    except Exception as e:
        logger.error(f"Erro fatal na extração: {e}")
        logger.error(f"Detalhes do erro: {traceback.format_exc()}")
    
    logger.info("Extração encerrada")
    
if __name__ == "__main__":
    logger.info("Iniciando processo de extração de dados F1 com processamento de dados meteorológicos")
    asyncio.run(main())