import sqlite3
import requests
from tqdm import tqdm
from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

@app.route('/')

@app.route('/homepage')
def home():
    return 'EEE MALING PANGSIT!!!'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

#Static Endpoint
@app.route('/trips/longest_trip_duration')
def route_longest_trip():
    conn = make_connection()
    longest_trip = get_longest_duration_trip(conn)
    return longest_trip.to_json()

@app.route('/trips/average_duration') 
def route_average_duration():
    conn = make_connection()
    result = get_average_duration(conn)
    return result.to_json()

@app.route('/trips/subscriber_type')    #return the longest trip (in minute) ever done
def subscriber_type():
    conn = make_connection()
    result = get_subscriber_type(conn)
    return result.to_json()

#Dynamic Endpoint
@app.route('/trips/longest_trip_duration/<bikeid>') # return the longest trip (in minute) ever done of a specific bike id
def route_longest_trip_bikeid(bikeid):
    conn = make_connection()
    longest_trip_bikeid = get_longest_duration_trip_bikeid(bikeid, conn)
    return longest_trip_bikeid.to_json()

@app.route('/trips/average_trip_duration/<bikeid>')
def route_average_trip_bikeid(bikeid):
    conn = make_connection()
    longest_trip_bikeid = get_average_duration_trip_bikeid(bikeid, conn)
    return longest_trip_bikeid.to_json()


# Create POST end points
@app.route('/trips/summary_by_date', methods = ['POST'])
def route_input_by_date():
    input_data = request.get_json() # Get the input as dictionary
    specified_date = input_data['period'] # Select specific items (period) from the dictionary (the value will be "2015-08")
    conn = make_connection()
    summary = summary_on_date(specified_date, conn)
    return summary

@app.route('/trips/summary_by_station', methods = ['POST'])
def route_input_by_month_station():
    input_data = request.get_json() # Get the input as dictionary
    specified_station = input_data['start_station'] 
    conn = make_connection()
    summary = summary_on_station(specified_station, conn)
    return summary



@app.route('/json', methods = ['POST']) 
def json_example():
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    name = req['name']
    age = req['age']
    address = req['address']
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

################### FUNCTIONS ####################
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_all_trips(conn):
    query = f""" SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_trips(data_trips, conn):
    query = f"""INSERT INTO trips values {data_trips}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

# Static Endpoint
def get_longest_duration_trip(conn):
    query = f"""SELECT duration_minutes FROM trips ORDER by duration_minutes DESC LIMIT 1"""
    result = pd.read_sql_query(query, conn)
    return result

def get_average_duration(conn):
    query = """SELECT duration_minutes FROM trips"""
    result = pd.read_sql_query(query, conn).mean()
    return result

def get_subscriber_type(conn):
    query = """SELECT subscriber_type, duration_minutes FROM trips"""
    selected_data = pd.read_sql_query(query, conn)
    result = selected_data.groupby('subscriber_type').agg(total_trip = ('subscriber_type', 'count'),
                                                     total_duration = ('duration_minutes','sum')).sort_values(by='total_trip', ascending = False)
    return result

# Dynamic Endpont
def get_longest_duration_trip_bikeid(bikeid, conn):
    query = f"""SELECT duration_minutes FROM trips WHERE bikeid = {bikeid} ORDER by duration_minutes DESC LIMIT 1"""
    result = pd.read_sql_query(query, conn)
    return result

def get_average_duration_trip_bikeid(bikeid, conn):
    query = f"""SELECT duration_minutes FROM trips WHERE bikeid = {bikeid}"""
    result = pd.read_sql_query(query, conn).mean()
    return result

# POST Endpoint
def summary_on_date(specified_date, conn):
    query = f"SELECT * FROM trips WHERE start_time LIKE '{specified_date}%'"
    selected_data = pd.read_sql_query(query, conn)
    result = selected_data.groupby('start_station_id').agg(
        bike_count = ('bikeid' , 'count'), 
        average_duration = ('duration_minutes' , 'mean'))
    return result.to_json()

def summary_on_station(specified_station, conn):
    query = f"""SELECT * FROM trips WHERE start_station_name LIKE '%{specified_station}%'"""
    selected_data = pd.read_sql_query(query, conn)
    result = selected_data.groupby('bikeid').agg(
        bike_count_start_from_the_station = ('bikeid' , 'count'), 
        average_duration = ('duration_minutes' , 'mean'))
    return result.to_json()

if __name__ == '__main__':
    app.run(debug=True, port=5000)