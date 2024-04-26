from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Road(Base):
    __tablename__ = 'Road'
    LinkID = Column(Integer, primary_key=True)
    RoadName = Column(String(255))
    RoadCategory = Column(String(50))
    StartLat = Column(Float)
    StartLon = Column(Float)
    EndLat = Column(Float)
    EndLon = Column(Float)


class BusStop(Base):
    __tablename__ = 'BusStop'
    BusStopCode = Column(Integer, primary_key=True)
    RoadName = Column(String(255))
    Description = Column(String)
    Latitude = Column(Float)
    Longitude = Column(Float)
    LinkID = Column(Integer, ForeignKey('Road.LinkID'))


class SpeedBand(Base):
    __tablename__ = 'SpeedBand'
    LinkID = Column(Integer, ForeignKey('Road.LinkID'), primary_key=True)
    SpeedBand = Column(Integer)
    Time = Column(TIMESTAMP, primary_key=True)


class TrafficImage(Base):
    __tablename__ = 'TrafficImage'
    CameraID = Column(Integer, primary_key=True)
    Latitude = Column(Float)
    Longitude = Column(Float)
    ImageLink = Column(String)
    LinkID = Column(Integer, ForeignKey('Road.LinkID'))
    Time = Column(TIMESTAMP, primary_key=True)


class BusDensity(Base):
    __tablename__ = 'BusDensity'
    BusStopCode = Column(Integer, ForeignKey(
        'BusStop.BusStopCode'), primary_key=True)
    Time = Column(TIMESTAMP, primary_key=True)
    NumBuses = Column(Integer)


class BusPassengerDensity(Base):
    __tablename__ = 'BusPassengerDensity'
    YearMonth = Column(String(7), primary_key=True)
    BusStopCode = Column(Integer, ForeignKey(
        'BusStop.BusStopCode'), primary_key=True)
    WeekdayTapIn = Column(Float)
    WeekendTapIn = Column(Float)
    WeekdayTapOut = Column(Float)
    WeekendTapOut = Column(Float)


class TaxiStand(Base):
    __tablename__ = 'TaxiStand'
    TaxiCode = Column(String(255), primary_key=True)
    Latitude = Column(Float)
    Longitude = Column(Float)
    Bfa = Column(String(255))
    Ownership = Column(String(255))
    Type = Column(String(255))
    Name = Column(String(255))
    LinkID = Column(Integer, ForeignKey('Road.LinkID'))


class MRTTrainStation(Base):
    __tablename__ = 'MRTTrainStation'
    TrainStationCode = Column(String(255), primary_key=True)
    Name = Column(String(255))
    MRTLine = Column(String(255))


class MRTTrainPassengerDensity(Base):
    __tablename__ = 'MRTTrainPassengerDensity'
    YearMonth = Column(String(7), primary_key=True)
    TrainStationCode = Column(String(255), ForeignKey(
        'MRTTrainStation.TrainStationCode'), primary_key=True)
    WeekdayTapIn = Column(Integer)
    WeekendTapIn = Column(Integer)
    WeekdayTapOut = Column(Integer)
    WeekendTapOut = Column(Integer)


class TrafficIncident(Base):
    __tablename__ = 'TrafficIncident'
    LinkID = Column(Integer, ForeignKey('Road.LinkID'))
    Type = Column(String(255))
    Latitude = Column(Float)
    Longitude = Column(Float)
    Message = Column(String, primary_key=True)
    Time = Column(TIMESTAMP)
