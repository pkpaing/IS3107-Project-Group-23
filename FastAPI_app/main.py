from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from connect_database import DBSession
from functions import *

# Create FastAPI app instance
app = FastAPI()

# Dependency to get database session


def get_db():
    try:
        db = DBSession()
        yield db
    finally:
        db.close()

# Define endpoints for GET Requests to retrieve data


@app.get("/roads/")
def read_roads(db: Session = Depends(get_db)):
    return get_roads(db)


@app.get("/bus_stops/")
def read_bus_stops(db: Session = Depends(get_db)):
    return get_bus_stops(db)


@app.get("/speed_bands/")
def read_speed_bands(db: Session = Depends(get_db)):
    return get_speed_bands(db)


@app.get("/traffic_images/")
def read_traffic_images(db: Session = Depends(get_db)):
    return get_traffic_images(db)


@app.get("/bus_density/")
def read_bus_densities(db: Session = Depends(get_db)):
    return get_bus_densities(db)


@app.get("/bus_passenger_density/")
def read_bus_passenger_densities(db: Session = Depends(get_db)):
    return get_bus_passenger_densities(db)


@app.get("/taxi_stands/")
def read_taxi_stands(db: Session = Depends(get_db)):
    return get_taxi_stands(db)


@app.get("/mrt_train_stations/")
def read_mrt_train_stations(db: Session = Depends(get_db)):
    return get_mrt_train_stations(db)


@app.get("/mrt_train_passenger_density/")
def read_mrt_train_passenger_densities(db: Session = Depends(get_db)):
    return get_mrt_train_passenger_densities(db)


@app.get("/traffic_incidents/")
def read_traffic_incidents(db: Session = Depends(get_db)):
    return get_traffic_incidents(db)

# Define endpoints for POST Requests to write data to db

# Endpoint to add multiple road data


@app.post("/roads/")
def add_roads(roads_data: List[dict], db: Session = Depends(get_db)):
    return create_roads(db, roads_data)

# Endpoint to add multiple bus stop data


@app.post("/bus_stops/")
def add_bus_stops(bus_stops_data: List[dict], db: Session = Depends(get_db)):
    return create_bus_stops(db, bus_stops_data)

# Endpoint to add multiple speed band data


@app.post("/speed_bands/")
def add_speed_bands(speed_bands_data: List[dict], db: Session = Depends(get_db)):
    return create_speed_bands(db, speed_bands_data)

# Endpoint to add multiple traffic image data


@app.post("/traffic_images/")
def add_traffic_images(traffic_images_data: List[dict], db: Session = Depends(get_db)):
    return create_traffic_images(db, traffic_images_data)

# Endpoint to add multiple bus density data


@app.post("/bus_density/")
def add_bus_densities(bus_densities_data: List[dict], db: Session = Depends(get_db)):
    return create_bus_densities(db, bus_densities_data)

# Endpoint to add multiple bus passenger density data


@app.post("/bus_passenger_density/")
def add_bus_passenger_densities(bus_passenger_densities_data: List[dict], db: Session = Depends(get_db)):
    return create_bus_passenger_densities(db, bus_passenger_densities_data)

# Endpoint to add multiple taxi stand data


@app.post("/taxi_stands/")
def add_taxi_stands(taxi_stands_data: List[dict], db: Session = Depends(get_db)):
    return create_taxi_stands(db, taxi_stands_data)

# Endpoint to add multiple MRT train station data


@app.post("/mrt_train_stations/")
def add_mrt_train_stations(mrt_train_stations_data: List[dict], db: Session = Depends(get_db)):
    return create_mrt_train_stations(db, mrt_train_stations_data)

# Endpoint to add multiple MRT train passenger density data


@app.post("/mrt_train_passenger_density/")
def add_mrt_train_passenger_densities(mrt_train_passenger_densities_data: List[dict], db: Session = Depends(get_db)):
    return create_mrt_train_passenger_densities(db, mrt_train_passenger_densities_data)

# Endpoint to add multiple traffic incident data


@app.post("/traffic_incidents/")
def add_traffic_incidents(traffic_incidents_data: List[dict], db: Session = Depends(get_db)):
    return create_traffic_incidents(db, traffic_incidents_data)
