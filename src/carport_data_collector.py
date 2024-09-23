import pandas as pd
from datetime import datetime, timedelta
import json
import os
import dotenv
import time
import mqtt_db_service as service
from influxdb import InfluxDBClient

def getInfluxClient(host, port, username, password, dbName):
    """
    Function to create an InfluxDB client connection.

    :param host: str - The hostname or IP address of the InfluxDB server.
    :param port: int - The port number for the InfluxDB server.
    :param username: str - Username for authenticating with InfluxDB.
    :param password: str - Password for authenticating with InfluxDB.
    :param dbName: str - Name of the database to connect to.
    :return: InfluxDBClient - Returns an instance of InfluxDBClient connected to the specified database.
    """
    return InfluxDBClient(host, port, username, password, dbName)

def getAllTables(client):
    """
    Function to retrieve all measurements (tables) from the InfluxDB.

    :param client: InfluxDBClient - Client connected to InfluxDB.
    :return: list - A list containing the names of all measurements (tables) in the database.
    """
    result = client.query("SHOW MEASUREMENTS")
    print(result)
    
    # Extracts measurement names from query results
    measurementNames = [item['name'] for item in result.get_points()]
    
    return measurementNames

def getValuesAsDataFrame(client, measurement, startTime, endTime):
    """
    Function to retrieve aggregated values from a measurement within a time range as a DataFrame.

    :param client: InfluxDBClient - Client connected to InfluxDB.
    :param measurement: str - The name of the measurement (table) in InfluxDB.
    :param startTime: str or datetime - The start time in ISO 8601 format or datetime.
    :param endTime: str or datetime - The end time in ISO 8601 format or datetime.
    :return: DataFrame - Pandas DataFrame with the aggregated values within the specified time range.
    """
    
    # Convert datetime objects to ISO 8601 format if necessary
    if isinstance(startTime, datetime):
        startTime = startTime.strftime('%Y-%m-%dT%H:%M:%SZ')
    if isinstance(endTime, datetime):
        endTime = endTime.strftime('%Y-%m-%dT%H:%M:%SZ')

    data = {
            'report' : "Success",
            'loggerRequestBeginTime' : datetime.datetime.now().isoformat()
        }

    # Query to retrieve mean values within the time range, grouped by minute
    query = f"""
    SELECT MEAN(*) 
    FROM {measurement} 
    WHERE time >= '{startTime}' AND time <= '{endTime}' 
    GROUP BY time(1m) fill(none)
    """
    
    result = client.query(query)
    
    # Convert query result to Pandas DataFrame
    values = list(result.get_points())

    data['loggerRequestEndTime'] = datetime.datetime.now().isoformat()
    if values:
        data['df_data'] = pd.DataFrame(values)
    else:
        data['df_data'] = pd.DataFrame()  # Returns an empty DataFrame if no data available
    
    return data

def measurementToTable(measurement):
    """
    Function to map a measurement name to its corresponding database table.

    :param measurement: str - The name of the measurement (InfluxDB table).
    :return: str or None - The corresponding table name in the database or None if not found.
    """
    
    if measurement == "bms":
        return "CARPORT_BMS_EVPV"
    elif measurement == "grid":
        return "CARPORT_GRID_EVPV"
    elif measurement == "inverter":
        return "CARPORT_INVERTER_EVPV"
    elif measurement == "load":
        return "CARPORT_LOAD_EVPV"
    elif measurement == "solar":
        return "CARPORT_PV_EVPV"
    elif measurement == "unicoba":
        return "CARPORT_UNICOBA_EVPV"
    else:
        return None

def getLastTimestamp(client, measurement):
    """
    Function to retrieve the most recent timestamp from a measurement in InfluxDB.

    :param client: InfluxDBClient - Client connected to InfluxDB.
    :param measurement: str - The name of the measurement (InfluxDB table).
    :return: datetime or None - The most recent timestamp or None if no data available.
    """
    
    query = f"SELECT * FROM {measurement} ORDER BY time DESC LIMIT 1"
    result = client.query(query)
    
    # Extract the timestamp of the most recent data point
    points = list(result.get_points())
    if points:
        timestampStr = points[0]['time']
        return datetime.strptime(timestampStr, '%Y-%m-%dT%H:%M:%SZ')  # Converts to datetime object
    else:
        return None

def cleanDataFrame(df):
    """
    Function to clean a DataFrame:
    1. Renames the 'time' column to 'TIMESTAMP'.
    2. Removes the 'mean_' prefix from other column names.
    3. Adjusts the 'TIMESTAMP' column format.

    :param df: DataFrame - The Pandas DataFrame to be cleaned.
    :return: DataFrame - The cleaned DataFrame with renamed and adjusted columns.
    """
    
    # Rename the 'time' column to 'TIMESTAMP'
    df = df.rename(columns={'time': 'TIMESTAMP'})
    
    # Remove 'mean_' prefix from columns except 'TIMESTAMP'
    df.columns = [col.replace('mean_', '') if col != 'TIMESTAMP' else col for col in df.columns]

    # Adjust the TIMESTAMP format
    df['TIMESTAMP'] = df['TIMESTAMP'].apply(lambda x: x.replace("T", " ").replace("Z", ""))
    
    return df

def healthCheck():
    """
    Function to perform a health check by writing a heartbeat timestamp to a temporary file.

    :return: None
    """
    
    with open('/tmp/heartbeat.txt', 'w') as f:
        f.write(str(time.time()))  # Writes the current time as a heartbeat

# Load environment variables from the .env file
dotenv.load_dotenv()

# Retrieve db creentials from environment variables
dbCredentials = os.getenv("DB_CREDENTIALS").split(',')

# Initialize the database service
service.initDBService(dbCredentials[0], dbCredentials[1], dbCredentials[2])

# Retrieve logger connections from environment variables
loggers = os.getenv("INFLUX_CLIENTES").split(';')

# Loop to continuously collect and process data
while True:
    for logger in loggers:
        connectionData = os.getenv("INFLUX_CLIENTES").split(',')
        waitTime = 900  # Default wait time of 900 seconds (15 minutes)
        
        # Ensures the connection will be closed with try-finally
        connection = None
        try:
            # Establish the connection to InfluxDB
            connection = getInfluxClient(connectionData[0], int(connectionData[1]), connectionData[2], connectionData[3], connectionData[4])
            
            # Get all measurements (tables) from InfluxDB
            measurements = getAllTables(connection)
            
            # Process each measurement
            for measurement in measurements:
                table = measurementToTable(measurement)
                
                # Get the start time from the server and end time from the logger
                startTime = service.getLastTimestamp(table=table)  # Retrieves the last timestamp from the database table
                endTime = getLastTimestamp(connection, measurement)  # Retrieves the most recent timestamp from the logger

                # Ensure that the time delta does not exceed one hour
                if endTime > startTime + timedelta(hours=1):
                    endTime = startTime + timedelta(hours=1)
                    waitTime = 1  # Shorten wait time to 1 second for faster processing
                
                # Retrieve the values as a DataFrame
                data = getValuesAsDataFrame(connection, measurement, startTime, endTime)
                data['df_data'] = cleanDataFrame(data['df_data'])  # Clean the DataFrame by renaming columns and adjusting timestamp format
                print(table)
                print(data['df_data'])
                print(service.sendDF(data['df_data'], table=table))
                healthCheck()  # Perform a health check by writing a heartbeat
                
        finally:
            # Ensure the connection is closed properly
            if connection:
                connection.close()
                print("Connection closed successfully.")
            else:
                print("Connection error")
                exit(0)

        print(f'Waiting {waitTime} seconds for new measurement')
        time.sleep(waitTime)


