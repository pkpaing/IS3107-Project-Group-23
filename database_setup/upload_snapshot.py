import os
from sqlalchemy.orm import scoped_session
# Importing DBSession from connect_database.py
from connect_database import DBSession
from models import Road, BusStop, SpeedBand, TrafficImage, BusDensity, BusPassengerDensity, TaxiStand, MRTTrainStation, MRTTrainPassengerDensity, TrafficIncident
import pandas as pd

# Function to load CSV data into the database


def load_csv_to_db(session, model, filepath):
    # Read CSV file into a pandas DataFrame
    df = pd.read_csv(filepath)

    # Convert DataFrame to list of dictionaries for bulk insert
    records = df.to_dict(orient='records')

    # Bulk insert into the database
    session.bulk_insert_mappings(model, records)
    session.commit()


def main():
    # Directory containing CSV files, adjust path as necessary
    csv_directory = '../data_processing/snapshot_data'

    # Mapping of model classes to their respective CSV files
    model_csv_mapping = {
        Road: 'road.csv',
        BusStop: 'bus_stop.csv',
        SpeedBand: 'speed_band.csv',
        TrafficImage: 'traffic_image.csv',
        BusDensity: 'bus_density.csv',
        BusPassengerDensity: 'bus_passenger_density.csv',
        TaxiStand: 'taxi_stand.csv',
        MRTTrainStation: 'mrt_train_station.csv',
        MRTTrainPassengerDensity: 'mrt_train_passenger_density.csv',
        TrafficIncident: 'traffic_incident.csv'
    }

    # Create a new scoped session
    session = scoped_session(DBSession)

    # Loop through the model-csv mapping
    for model, csv_file in model_csv_mapping.items():
        filepath = os.path.join(csv_directory, csv_file)
        if os.path.exists(filepath):
            print(f'Uploading data from {filepath} to {model.__tablename__}')
            load_csv_to_db(session, model, filepath)
            print(f'Data uploaded successfully to {model.__tablename__}!')
        else:
            print(f'File {filepath} does not exist. Skipping...')

    # Close the session
    session.remove()


if __name__ == "__main__":
    main()
