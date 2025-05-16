from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# Classes para representar os diferentes tipos de dados da F1
class Driver:
    def __init__(self, 
                 driver_number: int, 
                 name: Optional[str] = None,
                 team: Optional[str] = None,
                 country_code: Optional[str] = None,
                 **kwargs):
        self.driver_number = driver_number
        self.name = name
        self.team = team
        self.country_code = country_code
        # Armazena quaisquer atributos adicionais
        for key, value in kwargs.items():
            setattr(self, key, value)

class Session:
    def __init__(self,
                 session_key: int,
                 meeting_key: int,
                 name: Optional[str] = None,
                 date: Optional[datetime] = None,
                 circuit: Optional[str] = None,
                 **kwargs):
        self.session_key = session_key
        self.meeting_key = meeting_key
        self.name = name
        self.date = date
        self.circuit = circuit
        # Armazena quaisquer atributos adicionais
        for key, value in kwargs.items():
            setattr(self, key, value)

class LapData:
    def __init__(self,
                 driver_number: int,
                 lap_number: int,
                 lap_time: Optional[float] = None,
                 sector_1_time: Optional[float] = None,
                 sector_2_time: Optional[float] = None,
                 sector_3_time: Optional[float] = None,
                 speed_trap: Optional[int] = None,
                 timestamp: Optional[datetime] = None,
                 **kwargs):
        self.driver_number = driver_number
        self.lap_number = lap_number
        self.lap_time = lap_time
        self.sector_1_time = sector_1_time
        self.sector_2_time = sector_2_time
        self.sector_3_time = sector_3_time
        self.speed_trap = speed_trap
        self.timestamp = timestamp
        # Armazena quaisquer atributos adicionais
        for key, value in kwargs.items():
            setattr(self, key, value)

class Position:
    def __init__(self,
                 driver_number: int,
                 position: int,
                 timestamp: datetime,
                 **kwargs):
        self.driver_number = driver_number
        self.position = position
        self.timestamp = timestamp
        # Armazena quaisquer atributos adicionais
        for key, value in kwargs.items():
            setattr(self, key, value)

class TelemetryData:
    def __init__(self,
                 driver_number: int,
                 timestamp: datetime,
                 speed: Optional[int] = None,
                 rpm: Optional[int] = None,
                 gear: Optional[int] = None,
                 throttle: Optional[int] = None,
                 brake: Optional[int] = None,
                 drs: Optional[int] = None,
                 x: Optional[float] = None,
                 y: Optional[float] = None,
                 z: Optional[float] = None,
                 **kwargs):
        self.driver_number = driver_number
        self.timestamp = timestamp
        self.speed = speed
        self.rpm = rpm
        self.gear = gear
        self.throttle = throttle
        self.brake = brake
        self.drs = drs
        self.x = x
        self.y = y
        self.z = z
        # Armazena quaisquer atributos adicionais
        for key, value in kwargs.items():
            setattr(self, key, value)

class RaceControl:
    def __init__(self,
                 timestamp: datetime,
                 message: str,
                 category: Optional[str] = None,
                 flag: Optional[str] = None,
                 driver_number: Optional[int] = None,
                 **kwargs):
        self.timestamp = timestamp
        self.message = message
        self.category = category
        self.flag = flag
        self.driver_number = driver_number
        # Armazena quaisquer atributos adicionais
        for key, value in kwargs.items():
            setattr(self, key, value)

class Weather:
    def __init__(self,
                 timestamp: datetime,
                 air_temp: Optional[float] = None,
                 track_temp: Optional[float] = None,
                 humidity: Optional[float] = None,
                 pressure: Optional[float] = None,
                 wind_speed: Optional[float] = None,
                 wind_direction: Optional[int] = None,
                 rainfall: Optional[bool] = None,
                 **kwargs):
        self.timestamp = timestamp
        self.air_temp = air_temp
        self.track_temp = track_temp
        self.humidity = humidity
        self.pressure = pressure
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction
        self.rainfall = rainfall
        # Armazena quaisquer atributos adicionais
        for key, value in kwargs.items():
            setattr(self, key, value)