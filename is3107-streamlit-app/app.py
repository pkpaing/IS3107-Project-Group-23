from folium.plugins import HeatMapWithTime
from folium.plugins import MarkerCluster
import plotly.express as px
import datetime as datetime
import requests
import pandas as pd
import folium
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
import base64
from folium.plugins import HeatMap, MarkerCluster
from streamlit_folium import folium_static
import matplotlib.pyplot as plt
from PIL import Image
from io import BytesIO
from ultralytics import YOLO

# Set up the page configuration for the dashboard
st.set_page_config(page_title='IS3107 Dashboard', page_icon='images/title.png',
                   layout="wide", initial_sidebar_state="auto", menu_items=None)

# Function to read a CSV file and return a DataFrame


def read_file(file):
    df = pd.read_csv(file)
    return df

# Function to read HTML file content and return it as a string


def read_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
    return html_content

# Function to detect vehicles in an image using YOLO model


def detect_vehicles(image, model_path):
    model = YOLO(model_path)
    results = model.predict(image, classes=[2, 3, 5, 7])
    result = results[0]
    num_vehicles = len(result.boxes)
    image = Image.fromarray(result.plot()[:, :, ::-1])
    return image, num_vehicles

# Function to retrieve data from an API endpoint and return it as a DataFrame


def get_API_data(data):
    url = f"http://103.195.4.105:8000/{data}"
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.ok:
        # Check if response JSON is empty
        if response.json():
            df = pd.DataFrame(response.json())
            return df

# Function to refresh data from API endpoints and cache the results


@st.cache_data
def refresh_button(list):
    # If it is, run get_API_data
    datasets = list

    for dataset in datasets:
        data = get_API_data(dataset)
        if data is not None:
            data.to_csv(f"./mock_data/{dataset}.csv")
            print(f"{dataset} is updated")
            if dataset == 'mrt_train_stations':
                latitudes = []
                longitudes = []
                mrt_train_data = pd.read_csv(
                    "./mock_data/mrt_train_stations.csv")
                mrt_train_data_latlong = pd.read_csv(
                    './mock_data/mrt_train_stations_with_latlong.csv')
                if len(mrt_train_data) != len(mrt_train_data_latlong):
                    for name in mrt_train_data['Name']:
                        mrt_name = name + ' MRT Station'
                        latitude, longitude = get_lat_lng(mrt_name)
                        if latitude is None and longitude is None:
                            # Retry with 'LRT Station' appended
                            mrt_name = name + ' LRT Station'
                            latitude, longitude = get_lat_lng(mrt_name)
                        latitudes.append(latitude)
                        longitudes.append(longitude)

                    # Add latitude and longitude columns to the DataFrame
                    mrt_train_data['Latitude'] = latitudes
                    mrt_train_data['Longitude'] = longitudes
                    mrt_train_data.to_csv(
                        './mock_data/mrt_train_stations_with_latlong.csv')

# Function to retrieve latitude and longitude of a location using OneMap API


def get_lat_lng(location):
    # Replace 'Fill in OneMap Token' with your actual OneMap API token
    token = 'Fill in OneMap Token'
    headers = {"Authorization": token}

    url_template = "https://www.onemap.gov.sg/api/common/elastic/search?searchVal={location}&returnGeom=Y&getAddrDetails=Y&pageNum=1"

    url = url_template.format(location=location)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data['found'] > 0:
            result = data['results'][0]
            latitude = result['LATITUDE']
            longitude = result['LONGITUDE']
            return latitude, longitude
        else:
            return None, None
    else:
        return None, None

# =================
# OVERVIEW PAGE COMPONENTS
# =================

# Function to create a map showing MRT stations and taxi stands


@st.cache_data
def create_map(mrt_train_df, taxi_stand_df):
    # Set the center of the map
    CENTER = [1.3765401823882508, 103.85805501383963]

    # Create a map centered at the mean of the coordinates
    map_obj = folium.Map(location=CENTER, zoom_start=10,
                         tiles="CartoDB dark_matter")

    # Create feature groups for MRT stations and taxi stands
    mrt_station_group = folium.FeatureGroup(name="MRT Stations")
    taxi_stand_group = folium.FeatureGroup(name="Taxi Stands")

    # Add markers for MRT stations
    for index, row in mrt_train_df.iterrows():
        loc = (row['Latitude'], row['Longitude'])
        popup = f"<b>Train Station Code:</b> {row['TrainStationCode']}<br>" \
            f"<b>Name:</b> {row['Name']}<br>" \
            f"<b>MRT Line:</b> {row['MRTLine']}<br>" \
            f"<b>Latitude:</b> {row['Latitude']}<br>" \
            f"<b>Longitude:</b> {row['Longitude']}<br>"
        icon = folium.CustomIcon('images/train.png', icon_size=(10, 10))
        folium.Marker(location=loc, popup=popup,
                      icon=icon).add_to(mrt_station_group)

    # Add markers for taxi stands
    for index, row in taxi_stand_df.iterrows():
        loc = (row['Latitude'], row['Longitude'])
        popup = f"<b>Taxi Code:</b> {row['TaxiCode']}<br>" \
            f"<b>Name:</b> {row['Name']}<br>" \
            f"<b>Latitude:</b> {row['Latitude']}<br>" \
            f"<b>Longitude:</b> {row['Longitude']}<br>" \
            f"<b>Ownership:</b> {row['Ownership']}<br>" \
            f"<b>Bfa:</b> {row['Bfa']}<br>" \
            f"<b>Type:</b> {row['Type']}<br>" \
            f"<b>Link ID:</b> {row['LinkID']}"
        icon = folium.CustomIcon('images/taxistop.png', icon_size=(10, 10))
        folium.Marker(location=loc, popup=popup,
                      icon=icon).add_to(taxi_stand_group)

    # Add feature groups to the map
    mrt_station_group.add_to(map_obj)
    taxi_stand_group.add_to(map_obj)

    # Add layer control to the map
    folium.LayerControl().add_to(map_obj)

    # Create legend HTML with images from custom icons
    legend_html = """
    <div style='position: fixed; bottom: 20px; right: 10px; width: 150px; height: 80px; border:2px solid grey; z-index:9999; font-size:14px; background-color:white; opacity: 0.8;'>
    &nbsp; Legend <br>
    &nbsp; <img src="data:image/png;base64,{}" style="height: 20px; width: 20px;"> MRT Station<br>
    &nbsp; <img src="data:image/png;base64,{}" style="height: 20px; width: 20px;"> Taxi Stand<br>
    </div>
    """.format(
        base64.b64encode(open('images/train.png', 'rb').read()).decode(),
        base64.b64encode(open('images/taxistop.png', 'rb').read()).decode()
    )

    # Add legend to the map
    map_obj.get_root().html.add_child(folium.Element(legend_html))

    # Save the map to an HTML file
    map_path = './output/map_train_and_taxi.html'
    map_obj.save(map_path)

    return map_path

# Function to create a map showing bus stops


def create_map2(bus_stop_df):
    # Set the center of the map
    CENTER = [1.3765401823882508, 103.85805501383963]

    # Create a map centered at the mean of the coordinates
    map_obj = folium.Map(location=CENTER, zoom_start=10,
                         tiles="CartoDB dark_matter")

    # Create a marker cluster layer
    marker_cluster = MarkerCluster().add_to(map_obj)

    # Add markers for each bus stop to the marker cluster layer
    for index, row in bus_stop_df.iterrows():
        popup_text = f"<b>{row['Description']}</b><br>Road: {row['RoadName']}" + \
            f"<br>Bus Stop Code: {row['BusStopCode']}" + \
            f"<br>Link ID: {row['LinkID']}"
        folium.Marker(location=[row['Latitude'], row['Longitude']],
                      popup=popup_text).add_to(marker_cluster)

    # Save the map to an HTML file
    map_path = './output/map_bus.html'
    map_obj.save(map_path)

    return map_path

# Function to create a map showing speed bands


def create_map3(speed_band_df):
    # Set the center of the map
    CENTER = [1.3765401823882508, 103.85805501383963]

    # Create a map centered at the mean of the coordinates
    map_obj = folium.Map(location=CENTER, zoom_start=11,
                         tiles="CartoDB dark_matter")

    # Define a function to assign colors based on speed bands
    def assign_color(speed_band):
        if speed_band in [10, 20]:
            return "red"
        elif speed_band in [30, 40]:
            return "orange"
        elif speed_band in [50, 60]:
            return "yellow"
        else:
            return "green"

    # Add lines
    for index, row in speed_band_df.iterrows():
        popup_text = f"Start Location: ({row['StartLat']}, {row['StartLon']})" + \
            f"<br>End Location: ({row['EndLat']}, {row['EndLon']})<br>Speed: " + \
            f"{row['SpeedBand']}<br>Time: {row['Time']}"
        folium.PolyLine(
            locations=[(row["StartLat"], row["StartLon"]),
                       (row["EndLat"], row["EndLon"])],
            color=assign_color(row["SpeedBand"]),
            weight=5,  # Adjust line weight as needed
            opacity=0.7,  # Adjust line opacity as needed
            popup=popup_text  # Add popup
        ).add_to(map_obj)

    # Create a legend using HTML with colors
    legend_html2 = """
    <div style='position: fixed; bottom: 20px; right: 10px; width: 150px; height: 120px; border:2px solid grey; z-index:9999; font-size:14px; background-color:white; opacity: 0.8;'>
    &nbsp; Legend <br>
    &nbsp; <span style='color:red;'>&#9679;</span> Speed 10-20km/h<br>
    &nbsp; <span style='color:orange;'>&#9679;</span> Speed 30-40km/h<br>
    &nbsp; <span style='color:yellow;'>&#9679;</span> Speed 50-60km/h<br>
    &nbsp; <span style='color:green;'>&#9679;</span> Speed > 60km/h<br>
    </div>
    """

    # Add legend to the map
    map_obj.get_root().html.add_child(folium.Element(legend_html2))

    # Save the map to an HTML file
    map_path = './output/map_speedband.html'
    map_obj.save(map_path)

    return map_path

# =================
# Density Page Components
# =================

# Function to create heatmap


def create_heatmap(data, column):
    # Create Folium map centered on Singapore
    CENTER = [1.3765401823882508, 103.85805501383963]
    # Create a map centered at the mean of the coordinates
    sg_map = folium.Map(location=CENTER, zoom_start=10,
                        tiles="CartoDB dark_matter")
    data.fillna(0, inplace=True)

    heat_data = data[['Latitude', 'Longitude', column]
                     ].values.tolist()  # Get latitude and longitude columns

    # Add heatmap layer to the map
    HeatMap(heat_data, radius=10, blur=10,
            min_opacity=0.5, max_zoom=15).add_to(sg_map)

    return sg_map

# =================
# Traffic Camera Detection Components
# =================

# Function to create a real-time traffic map


@st.cache_data
def create_map_real_time(traffic_incident_df, traffic_image):
    CENTER = [1.3765401823882508, 103.85805501383963]
    # Create a map centered at the mean of the coordinates
    map_obj = folium.Map(location=CENTER, zoom_start=10,
                         tiles="CartoDB dark_matter")

    # Create feature groups for each category
    traffic_incident_group = folium.FeatureGroup(name="Traffic Incidents")
    traffic_image_group = folium.FeatureGroup(name="Traffic Cameras")

    # Add markers for traffic incidents
    for index, row in traffic_incident_df.iterrows():
        loc = (row['Latitude'], row['Longitude'])
        popup = f"<b>LinkID:</b> {row['LinkID']}<br>" \
            f"<b>Time:</b> {row['Time']}<br>" \
            f"<b>Latitude:</b> {row['Latitude']}<br>" \
            f"<b>Longitude:</b> {row['Longitude']}<br>" \
            f"<b>Type:</b> {row['Type']}<br>" \
            f"<b>Message:</b> {row['Message']}<br>"
        icon = folium.CustomIcon('images/warning.png', icon_size=(12, 12))
        folium.Marker(location=loc, popup=popup, icon=icon).add_to(
            traffic_incident_group)

    # Add markers for traffic cameras
    for index, row in traffic_image.iterrows():
        loc = (row['Latitude'], row['Longitude'])
        popup = f"<b>CameraID:</b> {row['CameraID']}<br>" \
            f"<b>Time:</b> {row['Time']}<br>" \
            f"<b>Latitude:</b> {row['Latitude']}<br>" \
            f"<b>Longitude:</b> {row['Longitude']}<br>" \
            f"<b>LinkID:</b> {row['LinkID']}<br>"
        icon = folium.CustomIcon('images/camera.png', icon_size=(12, 12))
        folium.Marker(location=loc, popup=popup, icon=icon).add_to(
            traffic_image_group)

    # Add feature groups to the map
    traffic_incident_group.add_to(map_obj)
    traffic_image_group.add_to(map_obj)

    # Add layer control to the map
    folium.LayerControl().add_to(map_obj)

    # Create legend HTML with images from custom icons
    legend_html = """
    <div style='position: fixed; bottom: 20px; right: 10px; width: 150px; height: 80px; border:2px solid grey; z-index:9999; font-size:14px; background-color:white; opacity: 0.8;'>
    &nbsp; Legend <br>
    &nbsp; <img src="data:image/png;base64,{}" style="height: 20px; width: 20px;"> Traffic Incident<br>
    &nbsp; <img src="data:image/png;base64,{}" style="height: 20px; width: 20px;"> Traffic Camera<br>
    </div>
    """.format(
        base64.b64encode(open('images/warning.png', 'rb').read()).decode(),
        base64.b64encode(open('images/camera.png', 'rb').read()).decode()

    )
    # Add legend to the map
    map_obj.get_root().html.add_child(folium.Element(legend_html))

    # Save the map to an HTML file
    map_path = './output/map_traffic.html'
    map_obj.save(map_path)

    return map_path

# Function to load an image from a URL


def load_image(url):
    response = requests.get(url)
    image_content = response.content
    return Image.open(BytesIO(image_content))

# =================================================================
# Dashboard Pages
# =================================================================

# =================
# OVERVIEW
# =================


def overview():
    # Read last refresh time from text file
    with open('./mock_data/overview_retrieval_time.txt', 'r') as f:
        last_refresh_time = f.read()
        st.write(f"Last refreshed: {last_refresh_time}")

    # Read data from CSV files
    bus_stop_data = read_file('./mock_data/bus_stops.csv')
    mrt_train_data = read_file(
        './mock_data/mrt_train_stations_with_latlong.csv')
    taxi_data = read_file('./mock_data/taxi_stands.csv')

    # Divide the screen into 3 columns
    col1, col2, col3 = st.columns(3)

    # Display total number of rows for each dataframe
    col1.metric("Bus Stops", bus_stop_data.shape[0])
    col2.metric("MRT Stations", mrt_train_data.shape[0])
    col3.metric("Taxi Stands", taxi_data.shape[0])

    # Divide the screen into 2 columns
    col1, col2 = st.columns(2)

    # Display the map on the left side
    with col1:
        st.write("#### Train Stations and Taxi Stands")

        map_html_path = './output/map_train_and_taxi.html'
        try:
            map_html_content = read_html_file(map_html_path)
            components.html(map_html_content, height=300)
        except:
            create_map(mrt_train_data, taxi_data)
            map_html_content = read_html_file(map_html_path)
            components.html(map_html_content, height=300)

    # Display the notes on the right side
    with col2:
        st.write("#### Bus Stops")
        map_html_path = "./output/map_bus.html"
        try:
            map_html_content = read_html_file(map_html_path)
            components.html(map_html_content, height=300)
        except:
            create_map2(bus_stop_data)
            map_html_content = read_html_file(map_html_path)
            components.html(map_html_content, height=300)

    # Display the speed of traffic flow
    st.write("#### Speed of Traffic Flow")

    # Get traffic images from API
    tempt_traffic_images = read_file("./mock_data/traffic_images.csv")
    tempt_traffic_images_linkid = tempt_traffic_images['LinkID'].unique()
    roads = read_file("./mock_data/roads.csv")
    speed_bands = read_file("./mock_data/speed_bands.csv")
    speed_bands_merged = pd.merge(roads, speed_bands, on="LinkID")
    filtered_speed_bands = speed_bands_merged[speed_bands_merged['LinkID'].isin(
        tempt_traffic_images_linkid)]
    filtered_speed_bands['SpeedBand'] = filtered_speed_bands['SpeedBand'] * 10
    map_html_path = "./output/map_speedband.html"
    try:
        map_html_content = read_html_file(map_html_path)
        components.html(map_html_content, height=400)
    except:
        create_map3(filtered_speed_bands)
        map_html_content = read_html_file(map_html_path)
        components.html(map_html_content, height=400)


# =================
# BUS DENSITY
# =================


def bus_density():
    # Read last refresh time from text file
    with open('./mock_data/bus_retrieval_time.txt', 'r') as f:
        last_refresh_time = f.read()
        st.write(f"Last refreshed: {last_refresh_time}")

    # Read bus stop and bus passenger density data
    bus_stop_data = read_file('./mock_data/bus_stops.csv')
    bus_passenger_data = read_file('./mock_data/bus_passenger_density.csv')

    # Merge bus dataset with bus stop latitude and longitude
    bus_data = pd.merge(bus_passenger_data, bus_stop_data,
                        on='BusStopCode', how='left')
    bus_data = bus_data.sort_values(by='YearMonth', ascending=False)
    # Format year and month information
    year_month_value = bus_data['YearMonth'].iloc[0]
    year_month_date = pd.to_datetime(year_month_value)
    formatted_date = year_month_date.strftime('%B %Y')

    # Calculate total tap-ins and tap-outs for bus stops
    bus_data['total_tap'] = bus_data['WeekdayTapIn'] + bus_data['WeekendTapIn'] + \
        bus_data['WeekdayTapOut'] + bus_data['WeekendTapOut']
    bus_data['total_tap_weekday'] = bus_data['WeekdayTapIn'] + \
        bus_data['WeekendTapIn']
    bus_data['total_tap_weekend'] = bus_data['WeekdayTapOut'] + \
        bus_data['WeekendTapOut']

    bus_data = bus_data.sort_values(by='total_tap', ascending=False)

    col1, col2 = st.columns(2)

    with col1:
        # Display the heatmap for bus passenger density
        st.write(f"#### {formatted_date} Bus Passenger Density HeatMap")
        filtered_data = bus_data[bus_data['YearMonth'] == year_month_value]
        weekday_tapin_heatmap = create_heatmap(filtered_data, "total_tap")
        folium_static(weekday_tapin_heatmap, height=300, width=500)

    with col2:
        st.write('#### Total Tap In/Out Over Time')

        # Group by YearMonth and sum the total_taps
        grouped_data = bus_data.groupby(
            'YearMonth')['total_tap'].sum().reset_index()

        # Plot the line chart using Plotly Express
        fig = px.line(grouped_data, x='YearMonth', y='total_tap')
        # Define the x-axis tick values
        x_ticks = grouped_data['YearMonth'].tolist()

        # Update x-axis ticks
        fig.update_xaxes(title_text='YearMonth', tickmode='array',
                         tickvals=x_ticks, tickformat='%b %Y')
        fig.update_yaxes(title_text='Total Tap In and Out')

        # Update layout to set height and width
        fig.update_layout(height=300, width=500,
                          margin=dict(l=50, r=50, t=50, b=50))

        # Render the chart using Streamlit
        st.plotly_chart(fig)

    # Display bus passenger density details in a table
    bus_data['YearMonth'] = pd.to_datetime(bus_data['YearMonth'], format='%Y-%m')
    bus_data = bus_data[bus_data['YearMonth'] == bus_data['YearMonth'].max()]
    bus_data_table = bus_data[['BusStopCode', "Description", "WeekdayTapIn",
                               "WeekdayTapOut", "WeekendTapIn", "WeekendTapOut", "total_tap"]].copy()
    bus_data_table['BusStopCode'] = bus_data_table['BusStopCode'].astype(str)
    bus_data_table.reset_index(drop=True, inplace=True)
    st.write("#### Bus Passenger Density Details")
    st.dataframe(bus_data_table,
                 column_config={
                     "total_tap": st.column_config.ProgressColumn(
                         "Total_Tap_In_And_Out",
                         format="%f",
                         max_value=max(bus_data_table['total_tap'])
                     )
                 },
                 height=220, width=1050)

    # Read bus density data
    bus_density_df = read_file('./mock_data/bus_density.csv')
    bus_density_df = bus_density_df.merge(bus_stop_data, on="BusStopCode")

    # Display the heatmap for bus density
    st.write("#### Bus Density HeatMap")
    bus_density = create_heatmap(bus_density_df, "NumBuses")
    folium_static(bus_density, height=300, width=1050)

    # Count the frequency of each number of buses
    bus_density_df['Frequency'] = bus_density_df.groupby(
        'NumBuses')['NumBuses'].transform('count')

    # Calculate average number of buses
    avg_num_buses = bus_density_df['NumBuses'].mean()

    # Plot bar graph for frequency of number of buses
    fig = px.bar(bus_density_df, x='NumBuses', y='Frequency',
                 labels={'NumBuses': 'Number of Buses', 'Frequency': 'Frequency'})

    # Add average line to the bar graph
    fig.add_vline(x=avg_num_buses, line_dash="dash", line_color="red",
                  annotation_text=f'Average Number of Buses: {avg_num_buses:.2f}', annotation_position="top")
    bus_density_df = bus_density_df.sort_values(by='NumBuses', ascending=False)
    st.write("#### Frequency of Number of Buses")
    st.plotly_chart(fig)

    # Display bus density details in a table
    st.write("#### Bus Density Details")
    bus_density_df.reset_index(drop=True, inplace=True)
    st.dataframe(bus_density_df[["BusStopCode", "Description", "RoadName", "NumBuses"]],
                 column_config={
                     "NumBuses": st.column_config.ProgressColumn(
                         "NumBuses",
                         format="%f",
                         max_value=max(bus_density_df['NumBuses'])
                     ),
    }, height=220, width=1050)


# =================
# TRAIN DENSITY
# =================

def train_density():
    # Read last refresh time from text file
    with open('./mock_data/train_retrieval_time.txt', 'r') as f:
        last_refresh_time = f.read()
        st.write(f"Last refreshed: {last_refresh_time}")

    # Read MRT train station and train passenger density data
    mrt_train_data = read_file(
        './mock_data/mrt_train_stations_with_latlong.csv')
    mrt_passenger_data = read_file(
        './mock_data/mrt_train_passenger_density.csv')

    # Merge MRT dataset with train station latitude and longitude
    mrt_data = pd.merge(mrt_passenger_data, mrt_train_data,
                        on='TrainStationCode', how='left')
    mrt_data = mrt_data.sort_values(by='YearMonth', ascending=False)

    # Calculate total tap-ins and tap-outs for train stations
    mrt_data['total_tap'] = mrt_data['WeekdayTapIn'] + mrt_data['WeekendTapIn'] + \
        mrt_data['WeekdayTapOut'] + mrt_data['WeekendTapOut']

    # Format year and month information
    year_month_value = mrt_data['YearMonth'].iloc[0]
    year_month_date = pd.to_datetime(year_month_value)
    formatted_date = year_month_date.strftime('%B %Y')

    col1, col2 = st.columns(2)

    with col1:
        # Display the heatmap for bus passenger density
        st.write(f"#### {formatted_date} Train Passenger Density HeatMap")
        filtered_data = mrt_data[mrt_data['YearMonth'] == year_month_value]
        weekday_tapin_heatmap = create_heatmap(filtered_data, "total_tap")
        folium_static(weekday_tapin_heatmap, height=300, width=500)

    with col2:
        st.write('#### Total Tap In/Out Over Time')

        # Group by YearMonth and sum the total_taps
        grouped_data = mrt_data.groupby(
            'YearMonth')['total_tap'].sum().reset_index()

        # Plot the line chart using Plotly Express
        fig = px.line(grouped_data, x='YearMonth', y='total_tap')
        # Define the x-axis tick values
        x_ticks = grouped_data['YearMonth'].tolist()

        # Update x-axis ticks
        fig.update_xaxes(title_text='YearMonth', tickmode='array',
                         tickvals=x_ticks, tickformat='%b %Y')
        fig.update_yaxes(title_text='Total Tap In and Out')

        # Update layout to set height and width
        fig.update_layout(height=300, width=500,
                          margin=dict(l=50, r=50, t=50, b=50))

        # Render the chart using Streamlit
        st.plotly_chart(fig)

    # Display train passenger density details in a table
    st.write("#### Train Passenger Density Details")
    mrt_data['YearMonth'] = pd.to_datetime(mrt_data['YearMonth'], format='%Y-%m')
    mrt_data = mrt_data[mrt_data['YearMonth'] == mrt_data['YearMonth'].max()]
    mrt_data = mrt_data.sort_values(by='total_tap', ascending=False)
    # Group by 'Name' and join the 'MRTLine' strings together for the same 'Name'
    mrt_data['MRTLine'] = mrt_data.groupby('Name')['MRTLine'].transform(lambda x: '/'.join(x.unique()))
    mrt_data = mrt_data.drop_duplicates(subset='Name').reset_index(drop=True)
    mrt_data.reset_index(drop=True, inplace=True)
    st.dataframe(mrt_data[['Name', "MRTLine", "WeekdayTapIn", "WeekdayTapOut", "WeekendTapIn", "WeekendTapOut", "total_tap"]],
                 column_config={
                     "total_tap": st.column_config.ProgressColumn(
                         "Total_Tap_In_And_Out",
                         format="%f",
                         max_value=max(mrt_data['total_tap'])
                     )
    },
        height=220, width=1050)

# =================
# Traffic Camera Detection
# =================


def real_time_monitor():
    # Read last refresh time from text file
    with open('./mock_data/real_time_retrieval_time.txt', 'r') as f:
        last_refresh_time = f.read()
        st.write(f"Last refreshed: {last_refresh_time}")

    # Read necessary data files
    speed_bands = read_file('./mock_data/speed_bands.csv')
    traffic_images = read_file('./mock_data/traffic_images.csv')
    traffic_incident = read_file('./mock_data/traffic_incidents.csv')
    road = read_file('./mock_data/roads.csv')

    # Merge speed bands data with road data
    speed_bands_merged = speed_bands.merge(road, on="LinkID", how='left')

    # Create two columns for layout
    col1, col2 = st.columns([3, 1])

    # Display the map in the left column
    with col1:
        st.write("#### Traffic Incidents and Traffic Camera Map")
        map_html_path = create_map_real_time(traffic_incident, traffic_images)
        map_html_content = read_html_file(map_html_path)
        components.html(map_html_content, height=300)

    # Display the pie chart in the right column
    with col2:
        st.write("#### Traffic Incident Type")

        # Calculate counts of each incident type
        incident_type_counts = traffic_incident['Type'].value_counts()

        # Calculate percentage of each incident type
        incident_percentages = (incident_type_counts /
                                incident_type_counts.sum()) * 100

        # Create a DataFrame with the percentage values
        incident_data = {'Type': incident_percentages.index,
                         'Percentage': incident_percentages.values}
        df_incident_percentages = pd.DataFrame(incident_data)

        # Plotting the pie chart using Plotly
        fig_incident = px.pie(df_incident_percentages, values='Percentage',
                              names='Type')

        # Customize the layout to remove legend and label the slices
        fig_incident.update_layout(showlegend=False)
        fig_incident.update_traces(textinfo='percent+label',
                                   insidetextfont=dict(size=12))

        # Adjusting the legend position and size
        fig_incident.update_layout(
            autosize=False,
            height=330,  # Set the height
            width=300,  # Set the width
        )

        # Display the pie chart
        st.plotly_chart(fig_incident)

    # Allow the user to select a camera ID
    selected_camera_id = st.selectbox(
        'Select Camera ID:', traffic_images['CameraID'])

    # Display the selected traffic image and the number of vehicles detected
    st.write(f"#### Traffic Image")
    col1, col2, col3 = st.columns([4.5, 1, 4.5])

    # Display the selected image in the left column
    with col1:
        row = traffic_images[traffic_images['CameraID']
                             == selected_camera_id].iloc[0]
        image = load_image(row['ImageLink'])
        st.image(image, caption=f"Average Speed: \
                 {int(speed_bands.loc[speed_bands['LinkID'] == row['LinkID'], 'SpeedBand'].iloc[0]) * 10} km/h", use_column_width=True)

    # Display the 'Detect Vehicles' button in the middle column
    with col2:
        for _ in range(4):  # Add empty spaces to push the button down
            st.write("")
        if st.button("Detect Vehicles"):
            with col3:
                with st.spinner("Detecting vehicles..."):
                    predicted_image, num_vehicles = detect_vehicles(
                        image, "models/yolov8x_trained.pt")
                    st.image(predicted_image, caption='Number of Vehicles: ' +
                             f'{num_vehicles}', use_column_width=True)

# =================================================================
# Run App
# =================================================================


def main():
    # Set up the sidebar title and data source
    st.sidebar.title(f'Traffic Congestion and Public Transport Analysis')
    st.sidebar.write("Data source: LTA")

    # Define the select box in the sidebar with the page options
    option = st.sidebar.selectbox('Choose a view', ('Overview', 'Bus Density', 'Train Density',
                                  'Traffic Camera Detection'), index=0)
    # Display the corresponding function based on the selected option
    if option == 'Overview':
        # Define a unique key for the refresh button
        refresh_button_key = "refresh_button_overview"

        # Check if the refresh button is clicked
        if st.sidebar.button("Refresh Data", key=refresh_button_key):
            # Run the function when the button is clicked
            refresh_button(["bus_stops", "mrt_train_stations", "taxi_stands"])
            # Record the current date and time
            current_time = datetime.datetime.now()
            with open('./mock_data/overview_retrieval_time.txt', 'w') as f:
                f.write(current_time.strftime('%Y-%m-%d %H:%M:%S'))

            # Read data files and perform necessary processing
            bus_stop_data = read_file('./mock_data/bus_stops.csv')
            mrt_train_data = read_file(
                './mock_data/mrt_train_stations_with_latlong.csv')
            taxi_data = read_file('./mock_data/taxi_stands.csv')
            tempt_traffic_images = get_API_data("traffic_images")
            tempt_traffic_images_linkid = tempt_traffic_images['LinkID'].unique(
            )
            roads = read_file("./mock_data/roads.csv")
            speed_bands = read_file("./mock_data/speed_bands.csv")
            speed_bands_merged = pd.merge(roads, speed_bands, on="LinkID")
            filtered_speed_bands = speed_bands_merged[speed_bands_merged['LinkID'].isin(
                tempt_traffic_images_linkid)]
            filtered_speed_bands['SpeedBand'] = filtered_speed_bands['SpeedBand'] * 10

            # Create maps and update the HTML files
            create_map(mrt_train_data, taxi_data)
            create_map2(bus_stop_data)
            create_map3(filtered_speed_bands)

        # Display the overview page
        overview()
        st.sidebar.write("Click on an icon on the map to view details")
        st.sidebar.write(
            "Filter function is available at the top right corner of the map to customize the display according to your preferences.")

    elif option == 'Bus Density':
        # Define a unique key for the refresh button
        refresh_button_key = "refresh_button_bus"
        # Check if the refresh button is clicked
        if st.sidebar.button("Refresh Data", key=refresh_button_key):
            # Run the function when the button is clicked
            refresh_button(
                ["bus_stops", "bus_passenger_density", "bus_density"])
            # Record the current date and time
            current_time = datetime.datetime.now()
            with open('./mock_data/bus_retrieval_time.txt', 'w') as f:
                f.write(current_time.strftime('%Y-%m-%d %H:%M:%S'))

        # Display the bus density page
        bus_density()

    elif option == 'Train Density':
        # Define a unique key for the refresh button
        refresh_button_key = "refresh_button_train"
        # Check if the refresh button is clicked
        if st.sidebar.button("Refresh Data", key=refresh_button_key):
            # Run the function when the button is clicked
            refresh_button(
                ["mrt_train_stations", "mrt_train_passenger_density"])
            # Record the current date and time
            current_time = datetime.datetime.now()
            with open('./mock_data/train_retrieval_time.txt', 'w') as f:
                f.write(current_time.strftime('%Y-%m-%d %H:%M:%S'))
        # Display the train density page
        train_density()

    elif option == 'Traffic Camera Detection':
        # Define a unique key for the refresh button
        refresh_button_key = "refresh_button_real_time"
        # Check if the refresh button is clicked
        if st.sidebar.button("Refresh Data", key=refresh_button_key):
            # Run the function when the button is clicked
            refresh_button(
                ["traffic_images", "traffic_incidents", "speed_bands"])
            # Record the current date and time
            current_time = datetime.datetime.now()
            with open('./mock_data/real_time_retrieval_time.txt', 'w') as f:
                f.write(current_time.strftime('%Y-%m-%d %H:%M:%S'))
        # Display the real-time monitoring page
        real_time_monitor()
        st.sidebar.write("Click on an icon on the map to view details")
        st.sidebar.write(
            "Filter function is available at the top right corner of the map to customize the display according to your preferences.")


if __name__ == "__main__":
    main()
