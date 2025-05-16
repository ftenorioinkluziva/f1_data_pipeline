import asyncio
import json
import os
import random
import time
from datetime import datetime, timedelta
import sys
from dotenv import load_dotenv

from loguru import logger

from transformer import F1DataTransformer
from supabase_loader import SupabaseLoader
from models import Driver, Position, TelemetryData, LapData, RaceControl, Weather, Session

# Configura o logger
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add("demo_pipeline.log", rotation="10 MB", level="DEBUG", retention="1 week")

class F1DataGenerator:
    """Gerador de dados simulados da F1 para demonstração do pipeline"""
    
    def __init__(self):
        self.drivers = []
        self.session = None
        self.lap_number = 1
        self.start_time = datetime.now()
        self.initialize_data()
    
    def initialize_data(self):
        """Inicializa dados base para a simulação"""
        # Cria sessão simulada
        self.session = Session(
            session_key=1234,
            meeting_key=5678,
            name="Demo Grand Prix - Race",
            date=datetime.now(),
            circuit="Supabase Circuit",
            type="Race",
            location="Cloud City",
            country_name="Dataland"
        )
        
        # Cria pilotos simulados
        team_data = [
            ("Mercedes", "#00D2BE"),
            ("Red Bull Racing", "#0600EF"),
            ("Ferrari", "#DC0000"),
            ("McLaren", "#FF8700"),
            ("Alpine", "#0090FF"),
            ("AlphaTauri", "#2B4562"),
            ("Aston Martin", "#006F62"),
            ("Williams", "#005AFF"),
            ("Alfa Romeo", "#900000"),
            ("Haas F1 Team", "#FFFFFF")
        ]
        
        driver_numbers = [44, 33, 16, 4, 3, 14, 63, 77, 11, 10, 22, 5, 18, 6, 23, 7, 47, 99, 9, 31]
        first_names = ["Lewis", "Max", "Charles", "Lando", "Daniel", "Fernando", "George", "Valtteri", 
                      "Sergio", "Pierre", "Yuki", "Sebastian", "Lance", "Nicholas", "Alex", "Kimi", 
                      "Mick", "Antonio", "Nikita", "Esteban"]
        last_names = ["Hamilton", "Verstappen", "Leclerc", "Norris", "Ricciardo", "Alonso", "Russell", 
                     "Bottas", "Perez", "Gasly", "Tsunoda", "Vettel", "Stroll", "Latifi", "Albon", 
                     "Raikkonen", "Schumacher", "Giovinazzi", "Mazepin", "Ocon"]
        countries = ["GBR", "NED", "MON", "GBR", "AUS", "ESP", "GBR", "FIN", "MEX", "FRA", 
                    "JPN", "GER", "CAN", "CAN", "THA", "FIN", "GER", "ITA", "RUS", "FRA"]
        
        for i in range(20):
            team_idx = i // 2  # 2 pilotos por equipe
            driver = Driver(
                driver_number=driver_numbers[i],
                name=f"{first_names[i]} {last_names[i]}",
                team=team_data[team_idx][0],
                country_code=countries[i],
                team_color=team_data[team_idx][1],
                first_name=first_names[i],
                last_name=last_names[i],
                short_name=first_names[i][:3].upper(),
                broadcast_name=last_names[i].upper()
            )
            self.drivers.append(driver)
    
    def generate_data_batch(self) -> list:
        """Gera um lote de dados simulados"""
        batch = []
        current_time = datetime.now()
        time_delta = (current_time - self.start_time).total_seconds()
        
        # Avança a volta a cada 90 segundos
        if time_delta > 0 and time_delta % 90 < 1:
            self.lap_number += 1
            logger.info(f"Avançando para a volta {self.lap_number}")
        
        # Gera dados de piloto
        if random.random() < 0.1:  # Ocasionalmente atualiza dados do piloto
            driver = random.choice(self.drivers)
            batch.append(json.dumps({
                "topic": "DriverList",
                "timestamp": current_time.isoformat(),
                "data": {
                    str(driver.driver_number): {
                        "Name": driver.name,
                        "TeamName": driver.team,
                        "Tla": driver.country_code,
                        "TeamColour": driver.team_color,
                        "FirstName": driver.first_name,
                        "LastName": driver.last_name,
                        "RacingNumber": str(driver.driver_number)
                    }
                }
            }))
        
        # Gera dados de sessão
        if random.random() < 0.05:  # Raramente atualiza dados da sessão
            batch.append(json.dumps({
                "topic": "SessionInfo",
                "timestamp": current_time.isoformat(),
                "data": {
                    "Key": self.session.session_key,
                    "MeetingKey": self.session.meeting_key,
                    "Name": self.session.name,
                    "StartDate": self.session.date.isoformat(),
                    "CircuitShortName": self.session.circuit,
                    "Type": self.session.type,
                    "Location": self.session.location,
                    "CountryName": self.session.country_name
                }
            }))
        
        # Gera dados de timing
        for driver in self.drivers:
            if random.random() < 0.3:  # 30% de chance por piloto por batch
                position = random.randint(1, 20)
                lap_time = random.uniform(80.0, 95.0)  # Tempo de volta entre 1:20 e 1:35
                s1_time = random.uniform(25.0, 30.0)
                s2_time = random.uniform(27.0, 32.0)
                s3_time = lap_time - s1_time - s2_time
                speed_trap = random.randint(280, 330)
                
                batch.append(json.dumps({
                    "topic": "TimingData",
                    "timestamp": current_time.isoformat(),
                    "data": {
                        str(driver.driver_number): {
                            "Position": str(position),
                            "NumberOfLaps": str(self.lap_number),
                            "LastLapTime": {
                                "Value": str(lap_time)
                            },
                            "Sector1Time": {
                                "Value": str(s1_time)
                            },
                            "Sector2Time": {
                                "Value": str(s2_time)
                            },
                            "Sector3Time": {
                                "Value": str(s3_time)
                            },
                            "BestSpeed": {
                                "Value": str(speed_trap)
                            }
                        }
                    }
                }))
        
        # Gera dados de posição
        for driver in self.drivers:
            if random.random() < 0.5:  # 50% de chance por piloto por batch
                x = random.uniform(-2000, 2000)
                y = random.uniform(-1000, 1000)
                z = random.uniform(-10, 10)
                
                batch.append(json.dumps({
                    "topic": "Position.z",
                    "timestamp": current_time.isoformat(),
                    "data": {
                        str(driver.driver_number): [x, y, z]
                    }
                }))
        
        # Gera dados do carro
        for driver in self.drivers:
            if random.random() < 0.4:  # 40% de chance por piloto por batch
                speed = random.randint(80, 330)
                rpm = random.randint(5000, 12000)
                gear = random.randint(1, 8)
                throttle = random.randint(0, 100) if random.random() < 0.7 else 0
                brake = 100 - throttle if throttle < 50 else 0
                drs = random.randint(0, 1)
                
                batch.append(json.dumps({
                    "topic": "CarData.z",
                    "timestamp": current_time.isoformat(),
                    "data": {
                        str(driver.driver_number): {
                            "Speed": speed,
                            "RPM": rpm,
                            "nGear": gear,
                            "Throttle": throttle,
                            "Brake": brake,
                            "DRS": drs
                        }
                    }
                }))
        
        # Gera mensagens de controle de corrida (raramente)
        if random.random() < 0.1:
            flag_types = ["YELLOW", "CLEAR", "BLUE", "CHEQUERED", "RED", "GREEN"]
            categories = ["Flags", "Other", "Incident", "Position"]
            
            flag = random.choice(flag_types)
            category = random.choice(categories)
            driver_number = random.choice(self.drivers).driver_number if random.random() < 0.5 else None
            
            messages = [
                "Track clear",
                "Yellow flag in sector 2",
                "Blue flag - faster car approaching",
                "Red flag - session stopped",
                "Investigation for exceeding track limits",
                "5-second time penalty",
                "DRS enabled",
                "Safety Car deployed",
                "Virtual Safety Car deployed",
                "Race will start at the top of the hour"
            ]
            
            batch.append(json.dumps({
                "topic": "RaceControlMessages",
                "timestamp": current_time.isoformat(),
                "data": [
                    {
                        "Message": random.choice(messages),
                        "Category": category,
                        "Flag": flag,
                        "DriverNumber": str(driver_number) if driver_number else None,
                        "Scope": "Track" if random.random() < 0.6 else "Driver",
                        "Sector": random.randint(1, 3) if random.random() < 0.3 else None,
                        "Lap": self.lap_number if random.random() < 0.5 else None
                    }
                ]
            }))
        
        # Gera dados meteorológicos (raramente)
        if random.random() < 0.05:
            batch.append(json.dumps({
                "topic": "WeatherData",
                "timestamp": current_time.isoformat(),
                "data": {
                    "AirTemp": str(random.uniform(18.0, 30.0)),
                    "TrackTemp": str(random.uniform(25.0, 50.0)),
                    "Humidity": str(random.uniform(30.0, 90.0)),
                    "Pressure": str(random.uniform(980.0, 1020.0)),
                    "WindSpeed": str(random.uniform(0.0, 20.0)),
                    "WindDirection": str(random.randint(0, 359)),
                    "Rainfall": "false" if random.random() < 0.8 else "true"
                }
            }))
        
        return batch

async def run_demo_pipeline():
    """Executa uma demonstração do pipeline com dados simulados"""
    logger.info("Iniciando demonstração do pipeline F1 para Supabase")
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Inicializa componentes
    data_generator = F1DataGenerator()
    transformer = F1DataTransformer()
    loader = SupabaseLoader()
    
    try:
        # Conecta ao Supabase
        logger.info("Conectando ao Supabase...")
        await loader.connect()
        logger.info("Conexão estabelecida")
        
        # Executa o loop de simulação
        logger.info("Iniciando geração de dados simulados...")
        
        # Contador para acompanhar o progresso
        batch_count = 0
        total_records = 0
        
        # Determina o número de lotes a serem gerados (padrão: 50 lotes)
        total_batches = int(os.getenv("DEMO_BATCHES", "50"))
        
        # Loop principal
        for i in range(total_batches):
            # Gera um lote de dados simulados
            raw_data = data_generator.generate_data_batch()
            
            # Transforma os dados
            transformed_data = transformer.process_data_batch(raw_data)
            
            # Conta registros
            batch_records = sum(len(value) for value in transformed_data.values())
            total_records += batch_records
            
            # Carrega no Supabase
            if batch_records > 0:
                await loader.load_batch(transformed_data)
                
                # Log dos tipos de dados carregados
                record_counts = {key: len(value) for key, value in transformed_data.items() if len(value) > 0}
                logger.info(f"Lote {i+1}/{total_batches}: {batch_records} registros - {record_counts}")
            else:
                logger.info(f"Lote {i+1}/{total_batches}: Sem dados para carregar")
            
            batch_count += 1
            
            # Pausa entre lotes (simulando o tempo real)
            await asyncio.sleep(0.2)
        
        logger.info(f"Demonstração concluída! {batch_count} lotes processados, {total_records} registros inseridos")
        
    except Exception as e:
        logger.error(f"Erro durante a demonstração: {e}")
    
    finally:
        # Fecha a conexão
        if hasattr(loader, 'pool') and loader.pool:
            logger.info("Fechando conexão com o Supabase...")
            await loader.disconnect()
            logger.info("Conexão fechada")

if __name__ == "__main__":
    asyncio.run(run_demo_pipeline())