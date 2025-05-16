import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional

from loguru import logger

from models import Driver, Session, LapData, Position, TelemetryData, RaceControl, Weather

class F1DataTransformer:
    """Transforma dados brutos da F1 em modelos estruturados"""
    
    def __init__(self):
        # Cache para armazenar informações de pilotos e sessão
        self.drivers_cache = {}
        self.session_info = None
    
    def process_data_batch(self, raw_lines: List[str]) -> Dict[str, List]:
        """Processa um lote de linhas de dados brutos e retorna dados estruturados"""
        if not raw_lines:
            return {}
        
        # Dicionário para armazenar os resultados transformados
        result = {
            'drivers': [],
            'sessions': [],
            'lap_data': [],
            'positions': [],
            'telemetry': [],
            'race_control': [],
            'weather': []
        }
        
        for line in raw_lines:
            try:
                # Ignora linhas vazias
                if not line.strip():
                    continue
                
                # Parse da linha JSON
                data = json.loads(line)
                
                # Determina o tipo de dados e processa adequadamente
                if 'topic' in data:
                    topic = data['topic']
                    
                    if topic == 'DriverList':
                        self._process_driver_list(data, result)
                    elif topic == 'SessionInfo':
                        self._process_session_info(data, result)
                    elif topic == 'TimingData':
                        self._process_timing_data(data, result)
                    elif topic == 'TimingAppData':
                        self._process_timing_app_data(data, result)
                    elif topic == 'Position.z':
                        self._process_position_data(data, result)
                    elif topic == 'CarData.z':
                        self._process_car_data(data, result)
                    elif topic == 'RaceControlMessages':
                        self._process_race_control(data, result)
                    elif topic == 'WeatherData':
                        self._process_weather_data(data, result)
                    # Outros tópicos podem ser adicionados conforme necessário
            
            except json.JSONDecodeError:
                logger.error(f"Erro ao decodificar JSON: {line[:100]}...")
            except Exception as e:
                logger.error(f"Erro ao processar linha: {str(e)}")
        
        # Remove duplicatas de drivers e sessions ao final
        if result['drivers']:
            result['drivers'] = self._deduplicate_by_attr(result['drivers'], 'driver_number')
        
        if result['sessions']:
            result['sessions'] = self._deduplicate_by_attr(result['sessions'], 'session_key')
        
        return result
    
    def _deduplicate_by_attr(self, items: List[Any], attr_name: str) -> List[Any]:
        """Remove duplicatas de uma lista com base em um atributo específico"""
        seen = set()
        unique_items = []
        
        for item in items:
            attr_value = getattr(item, attr_name, None)
            if attr_value not in seen:
                seen.add(attr_value)
                unique_items.append(item)
        
        return unique_items
    
    def _process_driver_list(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa dados de lista de pilotos"""
        if 'data' not in data:
            return
        
        for driver_number, driver_data in data['data'].items():
            try:
                # Converte o número do piloto para inteiro
                driver_number = int(driver_number)
                
                # Cria ou atualiza o objeto Driver
                driver = Driver(
                    driver_number=driver_number,
                    name=driver_data.get('Name', ''),
                    team=driver_data.get('TeamName', ''),
                    country_code=driver_data.get('Tla', ''),
                    team_color=driver_data.get('TeamColour', ''),
                    first_name=driver_data.get('FirstName', ''),
                    last_name=driver_data.get('LastName', ''),
                    short_name=driver_data.get('Tla', ''),
                    broadcast_name=driver_data.get('RacingNumber', '')
                )
                
                # Adiciona ao resultado e atualiza o cache
                result['drivers'].append(driver)
                self.drivers_cache[driver_number] = driver
                
            except (ValueError, TypeError) as e:
                logger.error(f"Erro ao processar dados do piloto {driver_number}: {e}")
    
    def _process_session_info(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa informações da sessão"""
        if 'data' not in data:
            return
        
        session_data = data['data']
        
        try:
            # Parse da data da sessão
            date_str = session_data.get('StartDate', '')
            session_date = datetime.fromisoformat(date_str) if date_str else None
            
            # Cria o objeto Session
            session = Session(
                session_key=int(session_data.get('Key', 0)),
                meeting_key=int(session_data.get('MeetingKey', 0)),
                name=session_data.get('Name', ''),
                date=session_date,
                circuit=session_data.get('CircuitShortName', ''),
                type=session_data.get('Type', ''),
                location=session_data.get('Location', ''),
                country_name=session_data.get('CountryName', '')
            )
            
            # Adiciona ao resultado e armazena no cache
            result['sessions'].append(session)
            self.session_info = session
            
        except Exception as e:
            logger.error(f"Erro ao processar informações da sessão: {e}")
    
    def _process_timing_data(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa dados de timing para extrair informações de voltas"""
        if 'data' not in data:
            return
        
        # Obtém o timestamp do evento
        timestamp_str = data.get('timestamp', '')
        timestamp = self._parse_timestamp(timestamp_str)
        
        for driver_number, timing_data in data['data'].items():
            try:
                driver_number = int(driver_number)
                
                # Processa dados de volta se presentes
                if 'LastLapTime' in timing_data:
                    lap_time_str = timing_data.get('LastLapTime', {}).get('Value', '')
                    lap_time = self._parse_lap_time(lap_time_str)
                    
                    # Obtém o número da volta atual
                    lap_number = int(timing_data.get('NumberOfLaps', 0))
                    
                    # Cria objeto LapData com os dados disponíveis
                    lap_data = LapData(
                        driver_number=driver_number,
                        lap_number=lap_number,
                        lap_time=lap_time,
                        timestamp=timestamp
                    )
                    
                    # Adiciona setores se disponíveis
                    for i in range(1, 4):
                        sector_key = f'Sector{i}Time'
                        if sector_key in timing_data:
                            sector_time_str = timing_data[sector_key].get('Value', '')
                            sector_time = self._parse_lap_time(sector_time_str)
                            setattr(lap_data, f'sector_{i}_time', sector_time)
                    
                    # Adiciona speed trap se disponível
                    if 'BestSpeed' in timing_data:
                        speed_str = timing_data['BestSpeed'].get('Value', '')
                        try:
                            speed_trap = int(speed_str)
                            lap_data.speed_trap = speed_trap
                        except (ValueError, TypeError):
                            pass
                    
                    result['lap_data'].append(lap_data)
                
                # Processa posição atual se presente
                if 'Position' in timing_data:
                    try:
                        position = int(timing_data['Position'])
                        pos_data = Position(
                            driver_number=driver_number,
                            position=position,
                            timestamp=timestamp
                        )
                        result['positions'].append(pos_data)
                    except (ValueError, TypeError):
                        pass
                
            except (ValueError, TypeError) as e:
                logger.error(f"Erro ao processar timing data para piloto {driver_number}: {e}")
    
    def _process_timing_app_data(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa dados do aplicativo de timing para informações adicionais de volta"""
        if 'data' not in data:
            return
        
        # Obtém o timestamp do evento
        timestamp_str = data.get('timestamp', '')
        timestamp = self._parse_timestamp(timestamp_str)
        
        for driver_number, app_data in data['data'].items():
            try:
                driver_number = int(driver_number)
                
                # Processa dados de setores e velocidade se presentes
                if 'Lines' in app_data:
                    for lap_info in app_data['Lines'].values():
                        lap_number = int(lap_info.get('NumberOfLaps', 0))
                        
                        # Verifica se já temos dados para esta volta
                        existing_laps = [lap for lap in result['lap_data'] 
                                         if lap.driver_number == driver_number and lap.lap_number == lap_number]
                        
                        if existing_laps:
                            # Atualiza a volta existente
                            lap_data = existing_laps[0]
                        else:
                            # Cria nova entrada de volta
                            lap_data = LapData(
                                driver_number=driver_number,
                                lap_number=lap_number,
                                timestamp=timestamp
                            )
                            result['lap_data'].append(lap_data)
                        
                        # Adiciona setores se disponíveis
                        for i in range(1, 4):
                            sector_key = f'Sector{i}'
                            if sector_key in lap_info:
                                sector_time_str = lap_info[sector_key].get('Value', '')
                                sector_time = self._parse_lap_time(sector_time_str)
                                setattr(lap_data, f'sector_{i}_time', sector_time)
                        
                        # Adiciona speed trap se disponível
                        if 'SpeedTrap' in lap_info:
                            speed_str = lap_info['SpeedTrap'].get('Value', '')
                            try:
                                speed_trap = int(speed_str)
                                lap_data.speed_trap = speed_trap
                            except (ValueError, TypeError):
                                pass
            
            except (ValueError, TypeError) as e:
                logger.error(f"Erro ao processar app data para piloto {driver_number}: {e}")
    
    def _process_position_data(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa dados de posição em pista"""
        if 'data' not in data:
            return
        
        # Obtém o timestamp do evento
        timestamp_str = data.get('timestamp', '')
        timestamp = self._parse_timestamp(timestamp_str)
        
        for driver_number, position_data in data['data'].items():
            try:
                driver_number = int(driver_number)
                
                if isinstance(position_data, list) and len(position_data) >= 2:
                    # Alguns sistemas fornecem posições como [x, y, z]
                    x, y = position_data[0], position_data[1]
                    z = position_data[2] if len(position_data) > 2 else 0
                    
                    # Cria objeto de telemetria com posição
                    telemetry = TelemetryData(
                        driver_number=driver_number,
                        timestamp=timestamp,
                        x=x,
                        y=y,
                        z=z
                    )
                    result['telemetry'].append(telemetry)
            except (ValueError, TypeError, IndexError) as e:
                logger.error(f"Erro ao processar dados de posição para piloto {driver_number}: {e}")
    
    def _process_car_data(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa dados do carro para telemetria"""
        if 'data' not in data:
            return
        
        # Obtém o timestamp do evento
        timestamp_str = data.get('timestamp', '')
        timestamp = self._parse_timestamp(timestamp_str)
        
        for driver_number, car_data in data['data'].items():
            try:
                driver_number = int(driver_number)
                
                # Verifica se temos dados de telemetria
                if isinstance(car_data, dict):
                    # Cria objeto de telemetria
                    telemetry = TelemetryData(
                        driver_number=driver_number,
                        timestamp=timestamp
                    )
                    
                    # Adiciona dados disponíveis
                    if 'Speed' in car_data:
                        telemetry.speed = int(car_data['Speed'])
                    
                    if 'RPM' in car_data:
                        telemetry.rpm = int(car_data['RPM'])
                    
                    if 'nGear' in car_data:
                        telemetry.gear = int(car_data['nGear'])
                    
                    if 'Throttle' in car_data:
                        telemetry.throttle = int(car_data['Throttle'])
                    
                    if 'Brake' in car_data:
                        telemetry.brake = int(car_data['Brake'])
                    
                    if 'DRS' in car_data:
                        telemetry.drs = int(car_data['DRS'])
                    
                    result['telemetry'].append(telemetry)
            except (ValueError, TypeError) as e:
                logger.error(f"Erro ao processar car data para piloto {driver_number}: {e}")
    
    def _process_race_control(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa mensagens do controle de corrida"""
        if 'data' not in data:
            return
        
        # Obtém o timestamp do evento
        timestamp_str = data.get('timestamp', '')
        timestamp = self._parse_timestamp(timestamp_str)
        
        for message_data in data['data']:
            try:
                msg = message_data.get('Message', '')
                category = message_data.get('Category', '')
                flag = message_data.get('Flag', '')
                
                # Tenta extrair o número do piloto da mensagem se presente
                driver_number = None
                if 'DriverNumber' in message_data:
                    try:
                        driver_number = int(message_data['DriverNumber'])
                    except (ValueError, TypeError):
                        pass
                
                # Cria o objeto RaceControl
                rc_message = RaceControl(
                    timestamp=timestamp,
                    message=msg,
                    category=category,
                    flag=flag,
                    driver_number=driver_number,
                    scope=message_data.get('Scope', ''),
                    sector=message_data.get('Sector'),
                    lap_number=message_data.get('Lap')
                )
                
                result['race_control'].append(rc_message)
            except Exception as e:
                logger.error(f"Erro ao processar mensagem de controle de corrida: {e}")
    
    def _process_weather_data(self, data: Dict, result: Dict[str, List]) -> None:
        """Processa dados meteorológicos"""
        if 'data' not in data:
            return
        
        weather_data = data['data']
        
        # Obtém o timestamp do evento
        timestamp_str = data.get('timestamp', '')
        timestamp = self._parse_timestamp(timestamp_str)
        
        try:
            # Extrai informações meteorológicas
            air_temp = self._parse_float(weather_data.get('AirTemp', ''))
            track_temp = self._parse_float(weather_data.get('TrackTemp', ''))
            humidity = self._parse_float(weather_data.get('Humidity', ''))
            pressure = self._parse_float(weather_data.get('Pressure', ''))
            wind_speed = self._parse_float(weather_data.get('WindSpeed', ''))
            wind_direction = self._parse_int(weather_data.get('WindDirection', ''))
            rainfall = weather_data.get('Rainfall', '').lower() == 'true'
            
            # Cria o objeto Weather
            weather = Weather(
                timestamp=timestamp,
                air_temp=air_temp,
                track_temp=track_temp,
                humidity=humidity,
                pressure=pressure,
                wind_speed=wind_speed,
                wind_direction=wind_direction,
                rainfall=rainfall
            )
            
            result['weather'].append(weather)
        
        except Exception as e:
            logger.error(f"Erro ao processar dados meteorológicos: {e}")
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Converte string de timestamp para objeto datetime"""
        if not timestamp_str:
            return datetime.now()
        
        try:
            # Formato ISO padrão
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            # Fallback para o timestamp atual
            return datetime.now()
    
    def _parse_lap_time(self, time_str: str) -> Optional[float]:
        """Converte string de tempo de volta para segundos"""
        if not time_str or time_str == '':
            return None
        
        try:
            # Verifica se já está em segundos
            return float(time_str)
        except (ValueError, TypeError):
            # Tenta converter de "MM:SS.mmm" para segundos
            try:
                # Pattern para "M:SS.mmm" ou "MM:SS.mmm"
                pattern = r'(\d+):(\d+\.\d+)'
                match = re.match(pattern, time_str)
                
                if match:
                    minutes = int(match.group(1))
                    seconds = float(match.group(2))
                    return minutes * 60 + seconds
                
                return None
            except Exception:
                return None
    
    def _parse_float(self, value: Any) -> Optional[float]:
        """Converte valor para float ou retorna None"""
        if value is None or value == '':
            return None
        
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