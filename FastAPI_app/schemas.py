from pydantic import BaseModel


class Road(BaseModel):
    LinkID: int
    RoadName: str
    RoadCategory: str
    StartLat: float
    StartLon: float
    EndLat: float
    EndLon: float


class BusStop(BaseModel):
    BusStopCode: int
    RoadName: str
    Description: str
    Latitude: float
    Longitude: float
    LinkID: int


class SpeedBand(BaseModel):
    LinkID: int
    SpeedBand: int
    Time: str


class TrafficImage(BaseModel):
    CameraID: int
    Latitude: float
    Longitude: float
    ImageLink: str
    LinkID: int
    Time: str


class BusDensity(BaseModel):
    BusStopCode: int
    Time: str
    NumBuses: int


class BusPassengerDensity(BaseModel):
    YearMonth: str
    BusStopCode: int
    WeekdayTapIn: float
    WeekendTapIn: float
    WeekdayTapOut: float
    WeekendTapOut: float


class TaxiStand(BaseModel):
    TaxiCode: str
    Latitude: float
    Longitude: float
    Bfa: str
    Ownership: str
    Type: str
    Name: str
    LinkID: int


class MRTTrainStation(BaseModel):
    TrainStationCode: str
    Name: str
    MRTLine: str


class MRTTrainPassengerDensity(BaseModel):
    YearMonth: str
    TrainStationCode: str
    WeekdayTapIn: int
    WeekendTapIn: int
    WeekdayTapOut: int
    WeekendTapOut: int


class TrafficIncident(BaseModel):
    LinkID: int
    Type: str
    Latitude: float
    Longitude: float
    Message: str
    Time: str
