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
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'email': ['e0544188@u.nus.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

@dag(dag_id='etl_traffic_data_taskflow_live', default_args=default_args, schedule_interval='0,15,30,45 * * * *', catchup=False, tags=['is3107_group_project_live'])
def etl_traffic_data_live_dag():
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
    
    ### 3. Traffic Incidents
    @task(task_id='extract_traffic_incidents_data')
    def extract_traffic_incidents_data():
        # Define the URL
        url = "http://datamall2.mytransport.sg/ltaodataservice/TrafficIncidents"

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
            
        traffic_incidents_df = pd.DataFrame(response.json()['value'])
    
        subfolder_name = "extracted_files"
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)  

        file_name = "extracted_traffic_incidents.csv"
        file_path = os.path.join(subfolder_path, file_name)
        traffic_incidents_df.to_csv(file_path, index=False)
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
    
    
    ### 7. Traffic Images
    @task(task_id='extract_traffic_images_data')
    def extract_traffic_images_data():
        # Define the URL
        url = "http://datamall2.mytransport.sg/ltaodataservice/Traffic-Imagesv2"

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

        traffic_images_df = pd.DataFrame(response.json()['value'])
        
        subfolder_name = "extracted_files"
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_name = "extracted_traffic_images.csv"
        file_path = os.path.join(subfolder_path, file_name)
        traffic_images_df.to_csv(file_path, index=False)
        return file_path

            
    ### 9. Bus Arrival
    @task(task_id='extract_bus_arrival_data')
    # Here we also read in the bus stop code from the outputs of one our previous tasks
    def extract_bus_arrival_data(bus_stops_file_path):
        # Define the URL
        url = "http://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2"

        # Define the API key (replace 'YOUR_API_KEY' with your actual API key)
        api_key = 'MSNUVOUfSEi1b+FVsTYo4A=='

        bus_arrivals_list = []

        bus_stops_df = pd.read_csv(bus_stops_file_path)
        # List of BusStopCodes to query
        busStopCodes = bus_stops_df['BusStopCode']

        # Loop through each BusStopCode and make a request
        for code in busStopCodes:
            # Set the headers with the API key
            headers = {
                "AccountKey": api_key,
                "accept": "application/json"
            }

            # Set the parameters with the BusStopCode
            params = {
                "BusStopCode": code
            }

            # Make the GET request
            response = requests.get(url, headers=headers, params=params)

            # Check if the request was successful
            if response.status_code == 200:
                # Print the response content
                bus_arrivals_list.append(response.json())
                # bus_arrivals_df = pd.DataFrame(bus_arrivals_list)
            else:
                print("Request failed with status code:", response.status_code, "for BusStopCode:", code)
            
            subfolder_name = "extracted_files"
            # Get the directory containing the Python script file
            script_directory = os.path.dirname(os.path.abspath(__file__))

            # Get the parent directory
            parent_directory = os.path.dirname(script_directory)

            subfolder_path = os.path.join(parent_directory, subfolder_name)
            os.makedirs(subfolder_path, exist_ok=True)     

            # file_name = "extracted_bus_arrivals.csv"
            # file_path = os.path.join(subfolder_path, file_name)
            # bus_arrivals_df.to_csv(file_path, index=False)
            json_file_path = os.path.join(subfolder_path, "extracted_bus_arrivals.json")
            # Write the bus_arrivals_list to a JSON file
            with open(json_file_path, 'w') as json_file:
                json.dump(bus_arrivals_list, json_file, indent=4)
            return json_file_path
    
    
    ### DATA TRANSFORMATION INTO ENTITIES & WRITING INTO DATABASE
    # Initial stage will just write into csvs first instead of writing into database
    
    @task(task_id='create_speed_band_entity')
    def create_speed_band_entity(speed_bands_file_path):
        speed_bands_df = pd.read_csv(speed_bands_file_path)
        speed_band = speed_bands_df[['LinkID', 'SpeedBand']]
        # Add a new column with the current time
        current_time = datetime.now()
        # Convert current_time to ISO formatted string
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        speed_band['Time'] = current_time_str
        file_name = "speed_band.csv"

        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        speed_band.to_csv(file_path, index=False)

        ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/speed_bands'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Generate composite key (LinkID + Time) for the database DataFrame
            db_df['CompositeKey'] = db_df['LinkID'].astype(str) + '_' + db_df['Time'].astype(str)
            
            # Generate composite key (LinkID + Time) for the current speed_band DataFrame
            speed_band['CompositeKey'] = speed_band['LinkID'].astype(str) + '_' + speed_band['Time'].astype(str)
            
            # Compare with the current speed_band DataFrame based on CompositeKey
            new_records = speed_band[~speed_band['CompositeKey'].isin(db_df['CompositeKey'])]

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

    @task(task_id='create_traffic_image_entity')
    def create_traffic_image_entity(traffic_images_file_path, speed_bands_file_path):
        # # These 3 steps are just to solve bug of PIL.UnidentifiedImageError
        # package_name = "contextily"
        # # Uninstall the package using pip
        # subprocess.check_call(["pip", "uninstall", "-y", package_name])
        # # Install the package using pip
        # subprocess.check_call(["pip", "install", package_name])

        speed_bands_df = pd.read_csv(speed_bands_file_path)
        traffic_image = pd.read_csv(traffic_images_file_path)

        ## MAP LINKID TO TRAFFIC IMAGE

        # First, prepare the data for NearestNeighbors
        coords_traffic_image = traffic_image[['Latitude', 'Longitude']]

        # Take each LinkID as middle of the Link segment
        coords_speed_bands = speed_bands_df[['StartLat', 'StartLon', 'EndLat', 'EndLon']]
        coords_speed_bands['Latitude'] = (speed_bands_df['StartLat'] + speed_bands_df['EndLat']) / 2
        coords_speed_bands['Longitude'] = (speed_bands_df['StartLon'] + speed_bands_df['EndLon']) / 2
        coords_speed_bands = coords_speed_bands[['Latitude', 'Longitude']]

        # Fit the NearestNeighbors model on the speed bands coordinates
        nn = NearestNeighbors(n_neighbors=1).fit(coords_speed_bands)

        # Find the closest LinkID for each traffic image
        distances, indices = nn.kneighbors(coords_traffic_image)

        # Assign the closest LinkID to each row in traffic_image
        traffic_image['LinkID'] = speed_bands_df.iloc[indices.flatten()]['LinkID'].values

        # Add a new column with the current time
        current_time = datetime.now()

        # Convert current_time to ISO formatted string
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        traffic_image['Time'] = current_time_str

        # Get the parent directory of the current script
        current_directory = os.path.dirname(os.path.abspath(__file__))
        parent_directory = os.path.dirname(current_directory)
        # Construct the path to the JSON file in the parent directory
        json_file_path = os.path.join(parent_directory, 'client_secrets.json')
        # Load service account credentials
        credentials = service_account.Credentials.from_service_account_file(json_file_path)
        drive_service = build('drive', 'v3', credentials=credentials)

        def upload_image_to_google_drive(image, parent_folder_id, current_datetime, filename):
            try:
                folder_name = current_datetime.strftime('%Y-%m-%d_%H-%M-%S')

                # Check if the subfolder already exists
                query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
                response = drive_service.files().list(q=query, fields='files(id)').execute()
                subfolders = response.get('files', [])

                if subfolders:
                    # Subfolder already exists, use its ID
                    subfolder_id = subfolders[0]['id']
                else:
                    # Subfolder doesn't exist, create it
                    folder_metadata = {
                        'name': folder_name,
                        'parents': [parent_folder_id],
                        'mimeType': 'application/vnd.google-apps.folder'
                    }
                    created_folder = drive_service.files().create(body=folder_metadata).execute()
                    subfolder_id = created_folder['id']

                    # Output the creation date and time
                    creation_time = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    print(f"Subfolder '{folder_name}' created at {creation_time}")

                file_metadata = {
                    'name': filename,
                    'parents': [subfolder_id]
                }

                # Convert PIL image to bytes
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format='JPEG')
                media_body = MediaIoBaseUpload(img_byte_arr, mimetype='image/jpeg')

                # Upload file to Google Drive
                uploaded_file = drive_service.files().create(body=file_metadata, media_body=media_body).execute()

            except Exception as e:
                print(f"Error uploading file: {e}")

        parent_folder_id = "1EcGRHhHU1ZEHf_xtJ2fsP9FIFSSk5u7c"
        current_datetime = datetime.now()
        # Iterate through traffic images
        for index, row in traffic_image.iterrows():
            image_url = row['ImageLink']
            response = requests.get(image_url)
            image = Image.open(io.BytesIO(response.content))

            filename = f'Traffic_Image_{row["CameraID"]}_{row["Time"]}.jpg'
            upload_image_to_google_drive(image, parent_folder_id, current_datetime, filename)
        
        # Save GoogleDrive image links to traffic_image.csv

        def list_images_in_timestamp_folder(parent_folder_id, current_datetime):
            try:
                # Format date and time in the format YYYY-MM-DD_HH-MM-SS
                folder_name = current_datetime.strftime('%Y-%m-%d_%H-%M-%S')

                # Check if the timestamp folder already exists
                query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
                response = drive_service.files().list(q=query, fields='files(id)').execute()
                timestamp_folders = response.get('files', [])

                if timestamp_folders:
                    # Timestamp folder exists, get its ID
                    timestamp_folder_id = timestamp_folders[0]['id']

                    # Query files in the timestamp folder
                    image_links = []
                    page_token = None
                    while True:
                        response = drive_service.files().list(q=f"'{timestamp_folder_id}' in parents and mimeType='image/jpeg'",
                                                            fields='nextPageToken, files(id, webContentLink)',
                                                            pageToken=page_token).execute()
                        for file in response.get('files', []):
                            image_links.append(file.get('webContentLink'))
                        page_token = response.get('nextPageToken')
                        if not page_token:
                            break

                    return image_links
                else:
                    print(f"Timestamp folder '{folder_name}' does not exist.")
                    return []

            except Exception as e:
                print(f"Error listing images in timestamp folder '{folder_name}': {e}")
                return []

        # List images in today's folder and update traffic_image DataFrame
        image_links_today = list_images_in_timestamp_folder(parent_folder_id, current_datetime)

        traffic_image['ImageLink'] = image_links_today

        file_name = "traffic_image.csv"
        
        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        traffic_image.to_csv(file_path, index=False)

        ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/traffic_images'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Generate composite key (CameraID + Time) for the database DataFrame
            db_df['CompositeKey'] = db_df['CameraID'].astype(str) + '_' + db_df['Time'].astype(str)
            
            # Generate composite key (CameraID + Time) for the current traffic_image DataFrame
            traffic_image['CompositeKey'] = traffic_image['CameraID'].astype(str) + '_' + traffic_image['Time'].astype(str)
            
            # Compare with the current traffic_image DataFrame based on CompositeKey
            new_records = traffic_image[~traffic_image['CompositeKey'].isin(db_df['CompositeKey'])]

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


    @task(task_id='create_bus_density_entity')
    def create_bus_density_entity(bus_arrival_file_path):
        # List to store the processed data
        data = []

        # Get the current time
        current_time = datetime.now()

        # Open the JSON file and load its contents
        with open(bus_arrival_file_path, 'r') as json_file:
            bus_arrivals_list = json.load(json_file)

        # Iterate through the list of bus arrivals
        for bus_stop_info in bus_arrivals_list:
            bus_stop_code = bus_stop_info['BusStopCode']
            num_buses = 0  # Initialize the number of buses arriving within the next minute

            # Iterate through the services at the bus stop
            for service in bus_stop_info['Services']:
                for key in ['NextBus', 'NextBus2', 'NextBus3']:
                    next_bus = service.get(key, {})
                    arrival_time_str = next_bus.get('EstimatedArrival', '')
                    
                    # Check if there's an arrival time
                    if arrival_time_str:
                        # Convert the arrival time to a datetime object
                        arrival_time = datetime.strptime(arrival_time_str, '%Y-%m-%dT%H:%M:%S+08:00')
                        
                        # Check if the bus is arriving within the next minute
                        if current_time <= arrival_time <= current_time + timedelta(minutes=5):
                            num_buses += 1

            # Add the data for this bus stop to the list
            data.append({'BusStopCode': bus_stop_code, 'NumBuses': num_buses, 'Time': current_time})
        
        # Convert the list of data to a DataFrame
        bus_density = pd.DataFrame(data)
        bus_density['Time'] = bus_density['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')

        file_name = "bus_density.csv"
        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        bus_density.to_csv(file_path, index=False)

        # ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/bus_density'

        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Generate composite key (BusStopCode + Time) for the database DataFrame
            db_df['CompositeKey'] = db_df['BusStopCode'].astype(str) + '_' + db_df['Time'].astype(str)
            
            # Generate composite key (BusStopCode + Time) for the current bus_density DataFrame
            bus_density['CompositeKey'] = bus_density['BusStopCode'].astype(str) + '_' + bus_density['Time'].astype(str)
            
            # Compare with the current bus_density DataFrame based on CompositeKey
            new_records = bus_density[~bus_density['CompositeKey'].isin(db_df['CompositeKey'])]

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
    
    @task(task_id='create_traffic_incident_entity')
    def create_traffic_incident_entity(traffic_incident_file_path, speed_bands_file_path):
        traffic_incident = pd.read_csv(traffic_incident_file_path)
        speed_bands_df = pd.read_csv(speed_bands_file_path)

        ## MAP LINKID TO TAXI STAND

        # First, prepare the data for NearestNeighbors
        coords_traffic_incidents = traffic_incident[['Latitude', 'Longitude']]

        # Take each LinkID as middle of the Link segment
        coords_speed_bands = speed_bands_df[['StartLat', 'StartLon', 'EndLat', 'EndLon']]
        coords_speed_bands['Latitude'] = (speed_bands_df['StartLat'] + speed_bands_df['EndLat']) / 2
        coords_speed_bands['Longitude'] = (speed_bands_df['StartLon'] + speed_bands_df['EndLon']) / 2
        coords_speed_bands = coords_speed_bands[['Latitude', 'Longitude']]

        # Fit the NearestNeighbors model on the speed bands coordinates
        nn = NearestNeighbors(n_neighbors=1).fit(coords_speed_bands)

        # Find the closest LinkID for each taxi stand
        distances, indices = nn.kneighbors(coords_traffic_incidents)

        # Assign the closest LinkID to each row in traffic_incident
        traffic_incident['LinkID'] = speed_bands_df.iloc[indices.flatten()]['LinkID'].values
        current_time = datetime.now()
        # Convert current_time to ISO formatted string
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        traffic_incident['Time'] = current_time_str

        file_name = "traffic_incident.csv"
        # Define the path to the subfolder, create the subfolder if it doesn't exist
        subfolder_name = 'entities'
        # Get the directory containing the Python script file
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Get the parent directory
        parent_directory = os.path.dirname(script_directory)

        subfolder_path = os.path.join(parent_directory, subfolder_name)
        os.makedirs(subfolder_path, exist_ok=True)

        file_path = os.path.join(subfolder_path, file_name)
        traffic_incident.to_csv(file_path, index=False)

        # ### BY RIGHT WHAT SHOULD BE GOING INTO DATABASE
        url = 'http://103.195.4.105:8000/traffic_incidents'
        response = requests.get(url)

        if response.status_code == 200:
            # Convert response data to DataFrame
            db_data = response.json()
            db_df = pd.DataFrame(db_data)
            
            # Compare with the current road DataFrame based on LinkID
            new_records = traffic_incident[~traffic_incident['Message'].isin(db_df['Message'])]

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


    
    bus_stops_data = extract_bus_stops_data()
    speed_bands_data = extract_speed_bands_data()
    traffic_images_data = extract_traffic_images_data()
    traffic_incidents_data = extract_traffic_incidents_data()

    create_speed_band_entity(speed_bands_data)
    create_traffic_image_entity(traffic_images_data, speed_bands_data)
    create_bus_density_entity(extract_bus_arrival_data(bus_stops_data))
    create_traffic_incident_entity(traffic_incidents_data, speed_bands_data)

# Instantiate the DAG
dag = etl_traffic_data_live_dag()