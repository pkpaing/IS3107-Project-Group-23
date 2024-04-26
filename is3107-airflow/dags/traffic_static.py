import requests
import pandas as pd
import zipfile
import io
from datetime import datetime, timedelta
from sklearn.neighbors import NearestNeighbors
import numpy as np
import os
import xlrd
from airflow.decorators import dag, task
import json
from PIL import Image
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'email': ['e0544188@u.nus.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

@dag(dag_id='etl_traffic_data_taskflow_static', default_args=default_args, schedule_interval='@monthly', catchup=False, tags=['is3107_group_project_static'])
def etl_traffic_data_static_dag():
    ### 1. Bus Stops
    @task(task_id='extract_bus_stops_data')
    def extract_bus_stops_data():
        API_KEY = 'MSNUVOUfSEi1b+FVsTYo4A=='
        BASE_URL = "http://datamall2.mytransport.sg/ltaodataservice/BusStops"

        headers = {
            "AccountKey": API_KEY,
            "accept": "application/json"
        }

        skip_value = 0
        all_bus_stops = []

        while True:
            url = f"{BASE_URL}?$skip={skip_value}"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                records = data.get('value', [])
                all_bus_stops.extend(records)

                if len(records) < 500:
                    break
                skip_value += 500

            else:
                print(f"Error {response.status_code}: {response.text}")
                break

        bus_stops_df = pd.DataFrame(all_bus_stops)

        # Create the subfolder if it doesn't exist
        subfolder_name = "extracted_files"

        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_name = "extracted_bus_stops.csv"
        file_path = os.path.join(subfolder_path, file_name)
        bus_stops_df.to_csv(file_path, index=False)
        return file_path
    
    ### 4. Speed Bands
    @task(task_id='extract_speed_bands_data')
    def extract_speed_bands_data():
        # If expired, get new one from LTA datamall
        API_KEY = 'MSNUVOUfSEi1b+FVsTYo4A=='
        BASE_URL = "http://datamall2.mytransport.sg/ltaodataservice/v3/TrafficSpeedBands"

        headers = {
            "AccountKey": API_KEY,
            "accept": "application/json"
        }

        skip_value = 0
        all_records = []

        while True:
            url = f"{BASE_URL}?$skip={skip_value}"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                records = data.get('value', [])
                all_records.extend(records)

                if len(records) < 500:
                    break
                skip_value += 500

            else:
                print(f"Error {response.status_code}: {response.text}")
                break
        
        all_records_df = pd.DataFrame(all_records).reset_index(drop = True)
        columns_to_convert = ['StartLat', 'StartLon', 'EndLat', 'EndLon']

        for column in columns_to_convert:
            all_records_df[column] = pd.to_numeric(all_records_df[column], errors='coerce')
        
        subfolder_name = "extracted_files"
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)  

        file_name = "extracted_speed_bands.csv"
        file_path = os.path.join(subfolder_path, file_name)
        all_records_df.to_csv(file_path, index=False)
        return file_path
    
    ### 5. Taxi Stands
    @task(task_id='extract_taxi_stands_data')
    def extract_taxi_stands_data():
        # Define the URL
        url = "http://datamall2.mytransport.sg/ltaodataservice/TaxiStands"

        # Define the API key (replace 'YOUR_API_KEY' with your actual API key)
        api_key = 'MSNUVOUfSEi1b+FVsTYo4A=='

        # Set the headers with the API key
        headers = {
            "AccountKey": api_key,
            "accept": "application/json"
        }

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Print the response content
            print(response.json())
        else:
            print("Request failed with status code:", response.status_code)

        taxi_stands_df = pd.DataFrame(response.json()['value'])
        
        subfolder_name = "extracted_files"
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_name = "extracted_taxi_stands.csv"
        file_path = os.path.join(subfolder_path, file_name)
        taxi_stands_df.to_csv(file_path, index=False)
        return file_path
    
    ### 10. Output static file of train_station_codes
    @task(task_id='extract_train_station_codes_data')
    def extract_train_station_codes_data():
        file_name = 'Train Station Codes and Chinese Names.xls'
        subdirectory_name = 'static_files'
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)
        subdirectory_path = os.path.join(parent_directory, subdirectory_name)
        # Search for the file within the subdirectory
        for root, dirs, files in os.walk(subdirectory_path):
            if file_name in files:
                # If the file is found, return its full path
                file_path = os.path.join(root, file_name)
                return file_path

        # If the file is not found, return None
        return None
    
    ### 2. Bus Monthly Passenger Data (ZIP FILE LINK)
    @task(task_id='extract_bus_monthly_passenger_data')
    def extract_bus_monthly_passenger_data():
        # Define the URL
        url = "http://datamall2.mytransport.sg/ltaodataservice/PV/Bus"

        # Define the API key (replace 'YOUR_API_KEY' with your actual API key)
        api_key = 'MSNUVOUfSEi1b+FVsTYo4A=='

        # Set the headers with the API key
        headers = {
            "AccountKey": api_key,
            "accept": "application/json"
        }

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the link to the zip file
            response_json = response.json()
            zip_file_url = response_json['value'][0]['Link']

            # Make a request to download the zip file
            zip_response = requests.get(zip_file_url)
            
            if zip_response.status_code == 200:
                # Use BytesIO for in-memory bytes buffer to handle the zip file
                zip_file_bytes = io.BytesIO(zip_response.content)

                # Use the zipfile library to open the zip file
                with zipfile.ZipFile(zip_file_bytes, 'r') as zip_ref:
                    # Extract the file into the current working directory
                    zip_ref.extractall("extracted_zip_files")
                    
                    # Assuming there is only one CSV file in the zip, get its name
                    csv_filename = zip_ref.namelist()[0]
                    
                    # Load the CSV file into a pandas DataFrame
                    bus_passengers_df = pd.read_csv(f"extracted_zip_files/{csv_filename}")
        else:
            print("Request failed with status code:", response.status_code)

        subfolder_name = "extracted_files"
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)    

        file_name = "extracted_bus_monthly_passenger.csv"
        file_path = os.path.join(subfolder_path, file_name)
        bus_passengers_df.to_csv(file_path, index=False)
        return file_path
    
    ### 8. Passenger by Train Station (ZIP FILE LINK)
    @task(task_id='extract_passenger_by_train_station_data')
    def extract_passenger_by_train_station_data():
        # Define the URL
        url = "http://datamall2.mytransport.sg/ltaodataservice/PV/Train"

        # Define the API key (replace 'YOUR_API_KEY' with your actual API key)
        api_key = 'MSNUVOUfSEi1b+FVsTYo4A=='

        # Set the headers with the API key
        headers = {
            "AccountKey": api_key,
            "accept": "application/json"
        }

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the link to the zip file
            response_json = response.json()
            zip_file_url = response_json['value'][0]['Link']

            # Make a request to download the zip file
            zip_response = requests.get(zip_file_url)
            
            if zip_response.status_code == 200:
                # Use BytesIO for in-memory bytes buffer to handle the zip file
                zip_file_bytes = io.BytesIO(zip_response.content)

                # Use the zipfile library to open the zip file
                with zipfile.ZipFile(zip_file_bytes, 'r') as zip_ref:
                    # Extract the file into the current working directory
                    zip_ref.extractall("extracted_zip_files")
                    
                    # Assuming there is only one CSV file in the zip, get its name
                    csv_filename = zip_ref.namelist()[0]
                    
                    # Load the CSV file into a pandas DataFrame
                    train_passengers_df = pd.read_csv(f"extracted_zip_files/{csv_filename}")
        else:
            print("Request failed with status code:", response.status_code)
        
        subfolder_name = "extracted_files"
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_name = "extracted_train_passengers.csv"
        file_path = os.path.join(subfolder_path, file_name)
        train_passengers_df.to_csv(file_path, index=False)
        return file_path
    
    @task(task_id='create_bus_stop_entity')
    def create_bus_stop_entity(bus_stops_file_path, speed_bands_file_path):
        speed_bands_df = pd.read_csv(speed_bands_file_path)
        bus_stop = pd.read_csv(bus_stops_file_path)

        ## MAP LINKID TO BUS STOP

        # First, prepare the data for NearestNeighbors
        coords_bus_stops = bus_stop[['Latitude', 'Longitude']]

        # Take each LinkID as middle of the Link segment
        coords_speed_bands = speed_bands_df[['StartLat', 'StartLon', 'EndLat', 'EndLon']]
        coords_speed_bands['Latitude'] = (speed_bands_df['StartLat'] + speed_bands_df['EndLat']) / 2
        coords_speed_bands['Longitude'] = (speed_bands_df['StartLon'] + speed_bands_df['EndLon']) / 2
        coords_speed_bands = coords_speed_bands[['Latitude', 'Longitude']]

        # Fit the NearestNeighbors model on the speed bands coordinates
        nn = NearestNeighbors(n_neighbors=1).fit(coords_speed_bands)

        # Find the closest LinkID for each bus stop
        distances, indices = nn.kneighbors(coords_bus_stops)

        # Assign the closest LinkID to each row in bus_stop_df
        bus_stop['LinkID'] = speed_bands_df.iloc[indices.flatten()]['LinkID'].values
        file_name = "bus_stop.csv"

        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        bus_stop.to_csv(file_path, index=False)

        ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/bus_stops'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Compare with the current bus_stop DataFrame based on BusStopCode
            new_records = bus_stop[~bus_stop['BusStopCode'].isin(db_df['BusStopCode'])]

            if not new_records.empty:
                # Prepare new records for POST request
                new_data = json.loads(new_records.to_json(orient='records'))
                
                # Make the POST request to add new records to the database
                post_response = requests.post(url, json=new_data)
                
                if post_response.status_code == 200:
                    print("New records successfully added to the database.")
                else:
                    print(f"Error: {post_response.status_code} - {post_response.text}")
            else:
                print("No new records to add.")
        else:
            print(f"Error: {response.status_code} - {response.text}")

    @task(task_id='create_road_entity')
    def create_road_entity(speed_bands_file_path):
        speed_bands_df = pd.read_csv(speed_bands_file_path)
        road = speed_bands_df[['LinkID', 'RoadName', 'RoadCategory', 'StartLon', 'StartLat', 'EndLon', 'EndLat']]
        file_name = "road.csv"

        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        road.to_csv(file_path, index=False)

        ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/roads'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Compare with the current road DataFrame based on LinkID
            new_records = road[~road['LinkID'].isin(db_df['LinkID'])]

            if not new_records.empty:
                # Prepare new records for POST request
                new_data = json.loads(new_records.to_json(orient='records'))
                
                # Make the POST request to add new records to the database
                post_response = requests.post(url, json=new_data)
                
                if post_response.status_code == 200:
                    print("New records successfully added to the database.")
                else:
                    print(f"Error: {post_response.status_code} - {post_response.text}")
            else:
                print("No new records to add.")
        else:
            print(f"Error: {response.status_code} - {response.text}")

    @task(task_id='create_taxi_stand_entity')
    def create_taxi_stand_entity(taxi_stand_file_path, speed_bands_file_path):
        taxi_stand = pd.read_csv(taxi_stand_file_path)
        speed_bands_df = pd.read_csv(speed_bands_file_path)

        ## MAP LINKID TO TAXI STAND

        # First, prepare the data for NearestNeighbors
        coords_taxi_stands = taxi_stand[['Latitude', 'Longitude']]

        # Take each LinkID as middle of the Link segment
        coords_speed_bands = speed_bands_df[['StartLat', 'StartLon', 'EndLat', 'EndLon']]
        coords_speed_bands['Latitude'] = (speed_bands_df['StartLat'] + speed_bands_df['EndLat']) / 2
        coords_speed_bands['Longitude'] = (speed_bands_df['StartLon'] + speed_bands_df['EndLon']) / 2
        coords_speed_bands = coords_speed_bands[['Latitude', 'Longitude']]

        # Fit the NearestNeighbors model on the speed bands coordinates
        nn = NearestNeighbors(n_neighbors=1).fit(coords_speed_bands)

        # Find the closest LinkID for each taxi stand
        distances, indices = nn.kneighbors(coords_taxi_stands)

        # Assign the closest LinkID to each row in taxi_stand
        taxi_stand['LinkID'] = speed_bands_df.iloc[indices.flatten()]['LinkID'].values

        file_name = "taxi_stand.csv"
        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        taxi_stand.to_csv(file_path, index=False)

        ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/taxi_stands'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Compare with the current road DataFrame based on LinkID
            new_records = taxi_stand[~taxi_stand['TaxiCode'].isin(db_df['TaxiCode'])]

            if not new_records.empty:
                # Prepare new records for POST request
                new_data = json.loads(new_records.to_json(orient='records'))
                
                # Make the POST request to add new records to the database
                post_response = requests.post(url, json=new_data)
                
                if post_response.status_code == 200:
                    print("New records successfully added to the database.")
                else:
                    print(f"Error: {post_response.status_code} - {post_response.text}")
            else:
                print("No new records to add.")
        else:
            print(f"Error: {response.status_code} - {response.text}")


    @task(task_id='create_mrt_train_station_entity')
    def create_mrt_train_station_entity(train_station_codes_file_path):
        train_station_codes = pd.read_excel(train_station_codes_file_path, engine='xlrd')
        mrt_train_station =  train_station_codes[['stn_code', 'mrt_station_english',
       'mrt_line_english']]

        mrt_train_station.columns = ['TrainStationCode', 'Name', 'MRTLine']

        file_name = "mrt_train_station.csv"
        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        mrt_train_station.to_csv(file_path, index=False)

        ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/mrt_train_stations'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Compare with the current road DataFrame based on LinkID
            new_records = mrt_train_station[~mrt_train_station['TrainStationCode'].isin(db_df['TrainStationCode'])]

            if not new_records.empty:
                # Prepare new records for POST request
                new_data = json.loads(new_records.to_json(orient='records'))
                
                # Make the POST request to add new records to the database
                post_response = requests.post(url, json=new_data)
                
                if post_response.status_code == 200:
                    print("New records successfully added to the database.")
                else:
                    print(f"Error: {post_response.status_code} - {post_response.text}")
            else:
                print("No new records to add.")
        else:
            print(f"Error: {response.status_code} - {response.text}")

    @task(task_id='create_bus_passenger_density_entity')
    def create_bus_passenger_density_entity(bus_monthly_passenger_file_path, bus_stops_file_path):
        bus_passengers_df = pd.read_csv(bus_monthly_passenger_file_path)
        bus_stop = pd.read_csv(bus_stops_file_path)

        bus_passenger_density = bus_passengers_df[['YEAR_MONTH', 'DAY_TYPE', 'PT_CODE',
       'TOTAL_TAP_IN_VOLUME', 'TOTAL_TAP_OUT_VOLUME']]

        bus_passenger_density.columns =  ['Year_Month', 'Day_Type', 'BusStopCode',
            'TOTAL_TAP_IN_VOLUME', 'TOTAL_TAP_OUT_VOLUME']

        # Pivoting the DataFrame to reshape it based on 'Day_Type'
        bus_passenger_density = bus_passenger_density.pivot_table(index=['Year_Month', 'BusStopCode'], 
                                        columns='Day_Type', 
                                        values=['TOTAL_TAP_IN_VOLUME', 'TOTAL_TAP_OUT_VOLUME'],
                                        aggfunc='sum').reset_index()

        # Rename the columns
        bus_passenger_density.columns = ['YearMonth', 'BusStopCode', 'WeekdayTapIn', 'WeekendTapIn', 
                            'WeekdayTapOut', 'WeekendTapOut']
        
        valid_bus_stops = [int(stop) for stop in bus_stop['BusStopCode']]

        bus_passenger_density = bus_passenger_density[bus_passenger_density['BusStopCode'].isin(valid_bus_stops)]
    
        file_name = "bus_passenger_density.csv"
        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        bus_passenger_density.to_csv(file_path, index=False)

        # ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/bus_passenger_density'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Generate composite key (BusStopCode + YearMonth) for the database DataFrame
            db_df['CompositeKey'] = db_df['BusStopCode'].astype(str) + '_' + db_df['YearMonth'].astype(str)
            
            # Generate composite key (BusStopCode + YearMonth) for the current bus_passenger_density DataFrame
            bus_passenger_density['CompositeKey'] = bus_passenger_density['BusStopCode'].astype(str) + '_' + bus_passenger_density['YearMonth'].astype(str)
            
            # Compare with the current bus_passenger_density DataFrame based on CompositeKey
            new_records = bus_passenger_density[~bus_passenger_density['CompositeKey'].isin(db_df['CompositeKey'])]

            if not new_records.empty:
                # Prepare new records for POST request
                new_records.drop(columns=['CompositeKey'], inplace=True)
                new_data = json.loads(new_records.to_json(orient='records'))
                
                # Make the POST request to add new records to the database
                post_response = requests.post(url, json=new_data)
                
                if post_response.status_code == 200:
                    print("New records successfully added to the database.")
                else:
                    print(f"Error: {post_response.status_code} - {post_response.text}")
            else:
                print("No new records to add.")
        else:
            print(f"Error: {response.status_code} - {response.text}")

    @task(task_id='create_mrt_train_passenger_density_entity')
    def create_mrt_train_passenger_density_entity(passenger_by_train_station_file_path):
        train_passengers_df = pd.read_csv(passenger_by_train_station_file_path)
        mrt_train_passenger_density = train_passengers_df[['YEAR_MONTH', 'DAY_TYPE', 'PT_CODE',
       'TOTAL_TAP_IN_VOLUME', 'TOTAL_TAP_OUT_VOLUME']]

        mrt_train_passenger_density.columns =  ['Year_Month', 'Day_Type', 'TrainStationCode',
            'TOTAL_TAP_IN_VOLUME', 'TOTAL_TAP_OUT_VOLUME']

        # Pivoting the DataFrame to reshape it based on 'Day_Type'
        mrt_train_passenger_density = mrt_train_passenger_density.pivot_table(index=['Year_Month', 'TrainStationCode'], 
                                        columns='Day_Type', 
                                        values=['TOTAL_TAP_IN_VOLUME', 'TOTAL_TAP_OUT_VOLUME'],
                                        aggfunc='sum').reset_index()

        # Rename the columns
        mrt_train_passenger_density.columns = ['YearMonth', 'TrainStationCode', 'WeekdayTapIn', 'WeekendTapIn', 
                            'WeekdayTapOut', 'WeekendTapOut']
        
        # Identify rows with TrainStationCode values containing a /
        split_rows = mrt_train_passenger_density[mrt_train_passenger_density['TrainStationCode'].str.contains('/')]

        # Duplicate rows with TrainStationCode containing a /
        duplicated_rows = split_rows.copy()

        # Split TrainStationCode values and assign them to two separate columns
        duplicated_rows['TrainStationCode'] = duplicated_rows['TrainStationCode'].str.split('/')

        # Duplicate rows with TrainStationCode containing a / and reset index
        duplicated_rows = duplicated_rows.explode('TrainStationCode').reset_index(drop=True)

        # Remove the original rows with 'TrainStationCode' values containing '/'
        mrt_train_passenger_density = mrt_train_passenger_density[~mrt_train_passenger_density['TrainStationCode'].str.contains('/')]

        # Append the modified duplicated rows to the original DataFrame
        mrt_train_passenger_density = pd.concat([mrt_train_passenger_density, duplicated_rows], ignore_index=True)

        file_name = "mrt_train_passenger_density.csv"
        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        mrt_train_passenger_density.to_csv(file_path, index=False)

        # ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/mrt_train_passenger_density'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Generate composite key (TrainStationCode + YearMonth) for the database DataFrame
            db_df['CompositeKey'] = db_df['TrainStationCode'].astype(str) + '_' + db_df['YearMonth'].astype(str)
            
            # Generate composite key (TrainStationCode + YearMonth) for the current mrt_train_passenger_density DataFrame
            mrt_train_passenger_density['CompositeKey'] = mrt_train_passenger_density['TrainStationCode'].astype(str) + '_' + mrt_train_passenger_density['YearMonth'].astype(str)
            
            # Compare with the current mrt_train_passenger_density DataFrame based on CompositeKey
            new_records = mrt_train_passenger_density[~mrt_train_passenger_density['CompositeKey'].isin(db_df['CompositeKey'])]

            if not new_records.empty:
                # Prepare new records for POST request
                new_records.drop(columns=['CompositeKey'], inplace=True)
                new_data = json.loads(new_records.to_json(orient='records'))
                
                # Make the POST request to add new records to the database
                post_response = requests.post(url, json=new_data)
                
                if post_response.status_code == 200:
                    print("New records successfully added to the database.")
                else:
                    print(f"Error: {post_response.status_code} - {post_response.text}")
            else:
                print("No new records to add.")
        else:
            print(f"Error: {response.status_code} - {response.text}")
    
    bus_stops_data = extract_bus_stops_data()
    speed_bands_data = extract_speed_bands_data()
    taxi_stands_data = extract_taxi_stands_data()
    train_station_codes_data = extract_train_station_codes_data()
    bus_monthly_passenger_data = extract_bus_monthly_passenger_data()
    passenger_by_train_station_data = extract_passenger_by_train_station_data()

    create_bus_stop_entity(bus_stops_data, speed_bands_data)
    create_road_entity(speed_bands_data)
    create_taxi_stand_entity(taxi_stands_data, speed_bands_data)
    create_mrt_train_station_entity(train_station_codes_data)
    create_bus_passenger_density_entity(bus_monthly_passenger_data, bus_stops_data)
    create_mrt_train_passenger_density_entity(passenger_by_train_station_data)

# Instantiate the DAG
dag = etl_traffic_data_static_dag()
