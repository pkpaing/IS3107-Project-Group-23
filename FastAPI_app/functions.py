from sqlalchemy.orm import Session
from models import Road, BusStop, SpeedBand, TrafficImage, BusDensity, BusPassengerDensity, TaxiStand, MRTTrainStation, MRTTrainPassengerDensity, TrafficIncident
from typing import List
from fastapi import HTTPException

# GET Requests to retrieve data


def get_roads(db: Session) -> List[Road]:
    return db.query(Road).all()


def get_bus_stops(db: Session) -> List[BusStop]:
    return db.query(BusStop).all()


def get_speed_bands(db: Session) -> List[SpeedBand]:
    return db.query(SpeedBand).all()


def get_traffic_images(db: Session) -> List[TrafficImage]:
    return db.query(TrafficImage).all()


def get_bus_densities(db: Session) -> List[BusDensity]:
    return db.query(BusDensity).all()


def get_bus_passenger_densities(db: Session) -> List[BusPassengerDensity]:
    return db.query(BusPassengerDensity).all()


def get_taxi_stands(db: Session) -> List[TaxiStand]:
    return db.query(TaxiStand).all()


def get_mrt_train_stations(db: Session) -> List[MRTTrainStation]:
    return db.query(MRTTrainStation).all()


def get_mrt_train_passenger_densities(db: Session) -> List[MRTTrainPassengerDensity]:
    return db.query(MRTTrainPassengerDensity).all()


def get_traffic_incidents(db: Session) -> List[TrafficIncident]:
    return db.query(TrafficIncident).all()

# POST Requests to update db

# Function to add multiple road data to the database


def create_roads(db: Session, road_data: List[dict]):
    try:
        db.bulk_insert_mappings(Road, road_data)
        db.commit()
        return road_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert road data: {e}")

# Function to add multiple bus stop data to the database


def create_bus_stops(db: Session, bus_stop_data: List[dict]):
    try:
        db.bulk_insert_mappings(BusStop, bus_stop_data)
        db.commit()
        return bus_stop_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert bus stop data: {e}")

# Function to add multiple speed band data to the database


def create_speed_bands(db: Session, speed_band_data: List[dict]):
    try:
        db.bulk_insert_mappings(SpeedBand, speed_band_data)
        db.commit()
        return speed_band_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert speed band data: {e}")

# Function to add multiple traffic image data to the database


def create_traffic_images(db: Session, traffic_image_data: List[dict]):
    try:
        db.bulk_insert_mappings(TrafficImage, traffic_image_data)
        db.commit()
        return traffic_image_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert traffic image data: {e}")

# Function to add multiple bus density data to the database


def create_bus_densities(db: Session, bus_density_data: List[dict]):
    try:
        db.bulk_insert_mappings(BusDensity, bus_density_data)
        db.commit()
        return bus_density_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert bus density data: {e}")

# Function to add multiple bus passenger density data to the database


def create_bus_passenger_densities(db: Session, bus_passenger_density_data: List[dict]):
    try:
        db.bulk_insert_mappings(BusPassengerDensity,
                                bus_passenger_density_data)
        db.commit()
        return bus_passenger_density_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert bus passenger density data: {e}")

# Function to add multiple taxi stand data to the database


def create_taxi_stands(db: Session, taxi_stand_data: List[dict]):
    try:
        db.bulk_insert_mappings(TaxiStand, taxi_stand_data)
        db.commit()
        return taxi_stand_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert taxi stand data: {e}")

# Function to add multiple MRT train station data to the database


def create_mrt_train_stations(db: Session, mrt_train_station_data: List[dict]):
    try:
        db.bulk_insert_mappings(MRTTrainStation, mrt_train_station_data)
        db.commit()
        return mrt_train_station_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert MRT train station data: {e}")

# Function to add multiple MRT train passenger density data to the database


def create_mrt_train_passenger_densities(db: Session, mrt_train_passenger_density_data: List[dict]):
    try:
        db.bulk_insert_mappings(MRTTrainPassengerDensity,
                                mrt_train_passenger_density_data)
        db.commit()
        return mrt_train_passenger_density_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert MRT train passenger density data: {e}")

# Function to add multiple traffic incident data to the database


def create_traffic_incidents(db: Session, traffic_incident_data: List[dict]):
    try:
        db.bulk_insert_mappings(TrafficIncident, traffic_incident_data)
        db.commit()
        return traffic_incident_data
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=400, detail=f"Failed to insert traffic incident data: {e}")
