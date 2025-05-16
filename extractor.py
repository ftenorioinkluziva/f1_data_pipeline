import asyncio
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from loguru import logger

from config import F1_DATA_FILE, F1_TOPICS, F1_TIMEOUT

class F1DataExtractor:
    """Extrator de dados da Fórmula 1 usando fastf1_livetiming"""
    
    def __init__(self, output_file: str = F1_DATA_FILE):
        self.output_file = output_file
        self.process = None
        self.last_position = 0
    
    async def start_extraction(self) -> None:
        """Inicia o processo de extração de dados da F1"""
        cmd = [sys.executable, "-m", "fastf1_livetiming", "save", self.output_file] + F1_TOPICS + ["--timeout", str(F1_TIMEOUT)]
        
        logger.info(f"Iniciando extração de dados da F1 para: {self.output_file}")
        logger.debug(f"Comando: {' '.join(cmd)}")
        
        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            logger.info(f"Processo de extração iniciado com PID: {self.process.pid}")
            
            # Monitora a saída do processo para debug
            while self.process.poll() is None:
                output = self.process.stdout.readline()
                if output:
                    logger.debug(f"F1 Extractor: {output.strip()}")
                await asyncio.sleep(0.1)
            
            # Verifica se o processo terminou com erro
            if self.process.returncode != 0:
                error = self.process.stderr.read()
                logger.error(f"Processo terminou com código de erro {self.process.returncode}: {error}")
                raise RuntimeError(f"Extração falhou: {error}")
            
            logger.info("Processo de extração concluído com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao executar a extração: {e}")
            if self.process and self.process.poll() is None:
                self.process.terminate()
            raise
    
    async def get_new_data(self) -> List[str]:
        """Recupera dados novos do arquivo de saída desde a última leitura"""
        if not os.path.exists(self.output_file):
            logger.warning(f"Arquivo de dados {self.output_file} ainda não existe")
            return []
        
        try:
            with open(self.output_file, 'r') as f:
                # Move para a última posição lida
                f.seek(self.last_position)
                
                # Lê as novas linhas
                new_lines = f.readlines()
                
                # Atualiza a posição
                self.last_position = f.tell()
            
            return new_lines
        except Exception as e:
            logger.error(f"Erro ao ler dados do arquivo: {e}")
            return []
    
    def stop_extraction(self) -> None:
        """Para o processo de extração"""
        if self.process and self.process.poll() is None:
            logger.info("Parando processo de extração...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
                logger.info("Processo de extração encerrado")
            except subprocess.TimeoutExpired:
                logger.warning("Processo não encerrou no tempo limite, forçando...")
                self.process.kill()