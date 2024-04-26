from sqlalchemy.orm import scoped_session
from connect_database import DBSession
from models import Road, BusStop

# Create a new scoped session
session = scoped_session(DBSession)

# Query all records from the Road table
all_roads = session.query(BusStop).all()
for road in all_roads:
    print(f'Road ID: {road.BusStopCode}, Name: {road.RoadName}')

# Query a specific BusStop by its code
bus_stop_code = 1421  # Example bus stop code
bus_stop = session.query(BusStop).filter(
    BusStop.BusStopCode == bus_stop_code).first()
if bus_stop:
    print(f'Bus Stop: {bus_stop.Description}')
else:
    print('Bus stop not found')

# Query BusStops with a specific road name
road_name = 'Victoria St'
bus_stops_on_road = session.query(BusStop).filter(
    BusStop.RoadName == road_name).all()
for bus_stop in bus_stops_on_road:
    print(f'Bus Stop: {bus_stop.Description} on {bus_stop.RoadName}')

# Close the session
session.remove()
