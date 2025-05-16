import asyncio
import os
import signal
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

from loguru import logger

from config import F1_DATA_FILE, BATCH_INTERVAL_MS
from extractor import F1DataExtractor
from transformer import F1DataTransformer
from loader import PostgreSQLLoader

class PerformanceMonitor:
    """Monitora a performance do pipeline"""
    
    def __init__(self):
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.total_lines_processed = 0
        self.total_records_inserted = 0
        self.batch_times = []
    
    def record_batch(self, lines_count: int, records_count: int, batch_time: float):
        """Registra os dados de um lote processado"""
        self.total_lines_processed += lines_count
        self.total_records_inserted += records_count
        self.batch_times.append(batch_time)
        
        # Limita o histórico para evitar uso excessivo de memória
        if len(self.batch_times) > 1000:
            self.batch_times = self.batch_times[-1000:]
    
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
            
            if recent_batches:
                avg_batch_time = sum(recent_batches) / len(recent_batches)
                max_batch_time = max(recent_batches)
                logger.info(f"Tempo médio de processamento por lote: {avg_batch_time*1000:.2f}ms")
                logger.info(f"Tempo máximo de processamento por lote: {max_batch_time*1000:.2f}ms")
                logger.info(f"Taxa de processamento: {len(recent_batches)/sum(recent_batches):.2f} lotes/s")
            
            self.last_report_time = current_time

# Configura o logger
logger.add("f1_pipeline.log", rotation="10 MB", level="INFO", retention="1 week")

# Flag para controlar o encerramento
shutdown_requested = False

def handle_shutdown(signum, frame):
    """Manipula solicitações de encerramento gracioso"""
    global shutdown_requested
    logger.info(f"Sinal de encerramento recebido ({signum})")
    shutdown_requested = True

async def main():
    """Função principal do pipeline ETL que orquestra o processo de extração,
    transformação e carga dos dados da F1 em tempo quase real."""
    
    # Registra manipuladores de sinais para encerramento gracioso
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Converte o intervalo de milissegundos para segundos para uso com asyncio.sleep
    batch_interval_sec = BATCH_INTERVAL_MS / 1000.0
    
    logger.info(f"Iniciando pipeline F1 com intervalo de processamento de {BATCH_INTERVAL_MS}ms")
    
    # Inicializa o monitor de performance
    perf_monitor = PerformanceMonitor()
    
    try:
        # Inicializa componentes do pipeline
        extractor = F1DataExtractor(output_file=F1_DATA_FILE)
        transformer = F1DataTransformer()
        loader = PostgreSQLLoader()
        
        # Conecta ao banco de dados
        logger.info("Estabelecendo conexão com o PostgreSQL/Supabase...")
        await loader.connect()
        logger.info("Conexão com o banco de dados estabelecida")
        
        # Inicia a extração em segundo plano
        logger.info(f"Iniciando extração de dados da F1 para arquivo: {F1_DATA_FILE}")
        extraction_task = asyncio.create_task(extractor.start_extraction())
        
        # Estatísticas de processamento para logs frequentes
        last_log_time = time.time()
        records_since_last_log = 0
        batches_since_last_log = 0
        empty_batches_count = 0
        
        # Contador de operações para o coração do loop
        heartbeat_counter = 0
        
        logger.info("Iniciando loop principal de processamento...")
        
        # Loop principal de processamento
        while not shutdown_requested:
            try:
                batch_start_time = time.time()
                
                # Obtém novos dados
                new_lines = await extractor.get_new_data()
                
                # Mostra sinal de vida periodicamente mesmo sem dados
                heartbeat_counter += 1
                if heartbeat_counter >= 1000:  # Aproximadamente a cada 100 segundos com intervalo de 100ms
                    logger.debug("Pipeline ativo, aguardando dados...")
                    heartbeat_counter = 0
                
                if new_lines:
                    # Processa os dados
                    transformed_data = transformer.process_data_batch(new_lines)
                    
                    # Conta registros do lote atual
                    current_batch_records = sum(len(value) for value in transformed_data.values())
                    records_since_last_log += current_batch_records
                    batches_since_last_log += 1
                    
                    # Carrega no banco de dados se houver dados transformados
                    if current_batch_records > 0:
                        await loader.load_batch(transformed_data)
                        
                        # Detalha os tipos de dados processados no log em modo debug
                        record_counts = {key: len(value) for key, value in transformed_data.items() if len(value) > 0}
                        if record_counts:
                            logger.debug(f"Processados: {record_counts}")
                    else:
                        empty_batches_count += 1
                        if empty_batches_count % 50 == 0:  # Log a cada 50 lotes vazios
                            logger.debug(f"Recebidos {empty_batches_count} lotes sem dados transformáveis")
                
                # Registra a duração do processamento do lote
                batch_duration = time.time() - batch_start_time
                perf_monitor.record_batch(
                    lines_count=len(new_lines) if new_lines else 0,
                    records_count=current_batch_records if 'current_batch_records' in locals() else 0,
                    batch_time=batch_duration
                )
                
                # Logs periódicos de atividade a cada 5 segundos, se houver atividade
                current_time = time.time()
                if current_time - last_log_time >= 5:
                    if records_since_last_log > 0:
                        logger.info(f"Últimos 5s: {batches_since_last_log} lotes, {records_since_last_log} registros")
                        avg_batch_ms = (current_time - last_log_time) * 1000 / batches_since_last_log if batches_since_last_log > 0 else 0
                        logger.info(f"Tempo médio por lote: {avg_batch_ms:.2f}ms, taxa: {records_since_last_log / 5:.1f} registros/s")
                    
                    # Gera relatório de performance completo a cada minuto
                    perf_monitor.report_if_needed()
                    
                    # Reseta contadores
                    records_since_last_log = 0
                    batches_since_last_log = 0
                    last_log_time = current_time
                
                # Calcula o tempo de sono adaptativo para manter o intervalo desejado
                elapsed = time.time() - batch_start_time
                sleep_time = max(0, batch_interval_sec - elapsed)
                
                # Pequena pausa adaptativa em milissegundos
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    # Se estamos atrasados, permite que o event loop respire
                    # usando sleep(0) para dar chance a outras tarefas assíncronas
                    await asyncio.sleep(0)
                    if elapsed > batch_interval_sec * 5:  # Se estiver muito atrasado, log de alerta
                        logger.warning(f"Processamento lento: {elapsed*1000:.2f}ms (meta: {BATCH_INTERVAL_MS}ms)")
            
            except asyncio.CancelledError:
                logger.info("Loop de processamento cancelado externamente")
                break
            except Exception as e:
                logger.error(f"Erro no loop de processamento: {e}")
                logger.debug(f"Detalhes do erro: {traceback.format_exc()}")
                # Espera um pouco mais em caso de erro para não sobrecarregar em caso de falhas
                await asyncio.sleep(1)
        
        # Encerramento do pipeline
        logger.info("Encerrando pipeline...")
        
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
        
        # Desconecta do banco de dados
        logger.info("Fechando conexão com o banco de dados...")
        await loader.disconnect()
        
        # Relatório final de performance
        logger.info("Gerando relatório final de performance...")
        perf_monitor.report_if_needed(force=True)
        
    except Exception as e:
        logger.error(f"Erro fatal no pipeline: {e}")
        logger.error(f"Detalhes do erro: {traceback.format_exc()}")
    
    logger.info("Pipeline encerrado")
    
if __name__ == "__main__":
    logger.info("Iniciando pipeline de dados F1")
    asyncio.run(main())

