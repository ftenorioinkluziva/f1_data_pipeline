import asyncio
import os
import signal
import time
import traceback
import ast
import base64
import zlib
import json
import argparse
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv

from loguru import logger
import asyncpg

# Configurar o logger
logger.remove()
logger.add("f1_telemetry.log", rotation="10 MB", level="DEBUG")
logger.add(lambda msg: print(msg), level="INFO")

# Flag para controlar o encerramento
shutdown_requested = False

def handle_shutdown(signum, frame):
    """Manipula solicitações de encerramento gracioso"""
    global shutdown_requested
    logger.info(f"Sinal de encerramento recebido ({signum})")
    shutdown_requested = True

def decode_compressed_data(encoded_data):
    """
    Decodifica dados da F1 que estão em formato comprimido (base64 + zlib)
    """
    try:
        # Remove aspas se presentes
        if isinstance(encoded_data, str) and encoded_data.startswith('"') and encoded_data.endswith('"'):
            encoded_data = encoded_data[1:-1]
            
        # Decodifica base64 e descomprime zlib com o wbits negativo (formato específico da F1)
        decoded_data = zlib.decompress(base64.b64decode(encoded_data), -zlib.MAX_WBITS)
        
        # Converte para JSON
        return json.loads(decoded_data)
    except Exception as e:
        logger.error(f"Erro ao decodificar dados: {e}")
        raise

class TelemetryProcessor:
    """Processa dados de telemetria dos carros e insere no banco de dados"""
    
    def __init__(self, session_id: int, conn_string: str):
        self.session_id = session_id
        self.conn_string = conn_string
        self.conn = None
        self.processed_count = 0
        self.connected = False
        self.drivers_processed = set()  # Para estatísticas
    
    async def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            self.conn = await asyncpg.connect(
                dsn=self.conn_string,
                ssl="require"
            )
            self.connected = True
            logger.info(f"Conexão com o banco de dados estabelecida para session_id={self.session_id}")
            
            # Verifica sessão
            try:
                session = await self.conn.fetchrow(
                    "SELECT id, name, type FROM public.sessions WHERE id = $1",
                    self.session_id
                )
                
                if session:
                    logger.info(f"Sessão encontrada: ID={session['id']}, Name={session['name']}, Type={session['type']}")
                else:
                    logger.warning(f"Sessão ID={self.session_id} não encontrada! Os dados serão inseridos mesmo assim.")
            except Exception as e:
                logger.error(f"Erro ao verificar sessão: {e}")
                logger.info("Continuando sem verificar sessão...")
        
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
    
    async def process_telemetry_data(self, topic: str, encoded_data: str, timestamp_str: str) -> int:
        """Processa dados de telemetria dos carros e retorna quantidade processada"""
        if not self.connected:
            await self.connect()
        
        if topic != 'CarData.z':
            return 0
        
        # Parse do timestamp
        if timestamp_str:
            try:
                # Primeiro parse como timezone-aware
                aware_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                # Depois converte para naive removendo a informação de fuso horário
                timestamp = aware_dt.replace(tzinfo=None)
            except ValueError:
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        # Decodifica os dados
        try:
            try:
                # Decodificar dados no formato específico da F1
                data = decode_compressed_data(encoded_data)
            except Exception as e:
                logger.error(f"Falha ao decodificar dados de telemetria: {e}")
                return 0
            
            telemetry_inserted = 0
            
            # Extrai os dados de telemetria
            if "Entries" in data:
                # Ciclo através de cada entrada de telemetria
                for entry in data["Entries"]:
                    if "Utc" in entry and "Cars" in entry:
                        # Converte o timestamp UTC de string para datetime
                        entry_time_str = entry["Utc"]
                        try:
                            # Converter o timestamp de string para datetime
                            entry_time = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00')).replace(tzinfo=None)
                        except ValueError:
                            logger.warning(f"Formato de timestamp inválido: {entry_time_str}, usando timestamp principal")
                            entry_time = timestamp
                        
                        # Ciclo através dos dados de cada carro
                        for driver_number, car_info in entry["Cars"].items():
                            if "Channels" in car_info:
                                channels = car_info["Channels"]
                                
                                # Extrai os canais específicos de telemetria
                                # Os números dos canais são baseados no processador CarDataProcessor
                                rpm = channels.get("0")
                                speed = channels.get("2")
                                gear = channels.get("3")
                                throttle = channels.get("4")
                                brake = channels.get("5")
                                drs = channels.get("45")
                                
                                # Insere no banco de dados
                                await self.conn.execute("""
                                    INSERT INTO public.car_telemetry (
                                        timestamp, utc_timestamp, session_id, driver_number,
                                        rpm, speed, gear, throttle, brake, drs,
                                        created_at, updated_at
                                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                """,
                                    timestamp, entry_time, self.session_id, str(driver_number),
                                    rpm, speed, gear, throttle, brake, drs,
                                    datetime.now(), datetime.now()
                                )
                                
                                telemetry_inserted += 1
                                self.drivers_processed.add(str(driver_number))
            
            if telemetry_inserted > 0:
                self.processed_count += telemetry_inserted
                # Log apenas a cada 50 processamentos para não sobrecarregar
                if self.processed_count % 50 == 0:
                    logger.info(f"Processados {self.processed_count} registros de telemetria até agora")
                    logger.info(f"Pilotos únicos rastreados: {len(self.drivers_processed)}")
            
            return telemetry_inserted
            
        except Exception as e:
            logger.error(f"Erro ao processar dados de telemetria: {e}")
            logger.debug(f"Detalhes: {traceback.format_exc()}")
            return 0
        
    async def close(self):
        """Fecha a conexão com o banco de dados"""
        if self.conn:
            await self.conn.close()
            logger.info("Conexão com o banco de dados fechada")

def parse_data_line(line: str) -> Tuple[str, Any, str]:
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

async def monitor_telemetry(input_file: str, session_id: int):
    """Monitora um arquivo de dados F1 para telemetria dos carros"""
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
    
    logger.info(f"Iniciando monitoramento de telemetria de carros para a sessão ID={session_id}")
    logger.info(f"Arquivo de entrada: {input_file}")
    
    # Inicializa o processador
    processor = TelemetryProcessor(session_id=session_id, conn_string=conn_string)
    
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
        telemetry_found = 0
        
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
                        
                        # Processsa cada linha
                        for line in lines:
                            try:
                                # Analisa a linha no formato específico
                                topic, data, timestamp = parse_data_line(line)
                                
                                # Se for dados de telemetria, processa
                                if topic == 'CarData.z':
                                    count = await processor.process_telemetry_data(topic, data, timestamp)
                                    telemetry_found += count
                            except ValueError:
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
                    logger.info(f"Registros de telemetria encontrados: {telemetry_found}")
                    logger.info(f"Registros de telemetria inseridos: {processor.processed_count}")
                    logger.info(f"Pilotos rastreados: {len(processor.drivers_processed)}")
                    
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
        logger.info(f"Registros de telemetria encontrados: {telemetry_found}")
        logger.info(f"Registros de telemetria inseridos: {processor.processed_count}")
        logger.info(f"Pilotos rastreados: {len(processor.drivers_processed)}")
    
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        logger.debug(traceback.format_exc())
    
    finally:
        # Fecha a conexão
        await processor.close()
        logger.info("Monitoramento encerrado")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Monitora telemetria dos carros da F1')
    parser.add_argument('--session-id', type=int, required=True, help='ID da sessão')
    parser.add_argument('--input-file', type=str, default='f1_data.txt', help='Arquivo de entrada')
    
    args = parser.parse_args()
    
    try:
        asyncio.run(monitor_telemetry(args.input_file, args.session_id))
    except KeyboardInterrupt:
        print("\nPrograma encerrado pelo usuário")