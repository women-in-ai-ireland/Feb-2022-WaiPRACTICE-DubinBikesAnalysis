import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np
import datetime as dt

def get_freq_counts(df):
    """
    Print out the top 10 most frequent and the bottom 10 least frequent categories for each column in the df
    
    df: Pandas Dataframe. Data of interest
    """
    for cols in df:
        print("======================")
        print(cols)
        tot = df[cols].value_counts()
        print(str(len(tot)) + " unique values")
        if len(tot) > 10:
            print("Top 10")
            print(tot.iloc[:10])
            print("Bottom 10")
            print(df[cols].value_counts(ascending=True).iloc[:10])
        else:
            print(tot)
            
def create_connection(db_file):
    """
    Create a database connection to the SQLite database specified by db_file
    db_file: database file
    
    Return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def get_bike_data(conn, start, end, station):
    """
    Get bike data
    
    conn: SQLite 3 connection
    start: String. Start date for query
    end: String. End date for query
    station: Int. ID of station of interest
    
    Returns: The required bike data
    """
    # If all stations are requested, adjust the station_id value for the SQL code
    if station == 777:
        sql = """
        SELECT
            *
        FROM bikes
        WHERE date BETWEEN '{0}' AND '{1}'
        """.format(start, end)
    else:
        sql = """
        SELECT
            *
        FROM bikes
        WHERE date BETWEEN '{0}' AND '{1}'
        AND "station id" = {2}
        """.format(start, end, station)

    df = pd.read_sql_query(sql, conn)
    df['DATETIME'] = [dt.datetime.strptime(d, "%Y-%m-%d %H:%M:%S") for d in df["DATETIME"]]
    df['DATE'] = df.DATETIME.dt.date
    return df

def get_station_data(conn, station):
    # If all stations are requested, adjust the station_id value for the SQL code
    if station == 777:
        sql = """
        SELECT
            *
        FROM stations
        """
    else:
        sql = """
        SELECT
            *
        FROM stations
        WHERE "station id" = {0}
        """.format(station)

    return pd.read_sql_query(sql, conn)

def get_weather_data(conn, start, end):
    sql = """
    SELECT
        *
    FROM weather
    WHERE date BETWEEN '{0}' AND '{1}'
    """.format(start, end)
    df = pd.read_sql_query(sql, conn)
    df['DATETIME'] = [dt.datetime.strptime(d, "%Y-%m-%d %H:%M:%S") for d in df["DATETIME"]]
    return df


def expand_weather_data(df, start, end):
    """
    Expand the weather data out to 5 min intervals
    
    Weather data is on an hourly level but bike transaction data is on a 5 min level.

    'rain': percipitation amount (mm) for the given hour. Therefore, divide by 12 to get the average volume for a 5 min interval
    'sun': sun duration (h) i.e. it is normalised to the hour so could easily apply the figure to the 5 min intervals

    This leaves the remaining variables - we will make the assumption they remained constant over the hour.

 
    Therefore:
    1. divide by 12 to get the average volume for a 5 min interval
    2. generate 5 min intervals
    3. expand weather data out to 5 min intervals
    """
    df['avg_rain_per_interval'] = df.rain / 12

    time_range = pd.date_range(start, end, freq='5min')
    time_range_df = pd.DataFrame(data={'DATETIME': np.array(time_range)})

    # Perform a merge on the date and hour combination
    df['DATE'] = df.DATETIME.dt.date
    df['HOUR'] = df.DATETIME.dt.hour
    

    time_range_df['DATE'] = time_range_df.DATETIME.dt.date
    time_range_df['HOUR'] = time_range_df.DATETIME.dt.hour
    time_range_df['MINUTE'] = time_range_df.DATETIME.dt.minute

    use_cols = ['rain', 'temp', 'wetb', 'dewpt', 'vappr', 'rhum',
           'msl', 'wdsp', 'wddir', 'sun', 'avg_rain_per_interval', 'DATE', 'HOUR']
    return time_range_df.merge(df[use_cols], on=['DATE', 'HOUR'], how='left')


def get_required_data(conn, start, end, station):
    bike_df = get_bike_data(conn, start, end, station)
    stations_df = get_station_data(conn, station)
    weather_df = get_weather_data(conn, start, end)
    exp_weather_df = expand_weather_data(weather_df, start, end)
    bike_station = bike_df.merge(stations_df, on='STATION ID', how='left')
    weather_cols = ['rain', 'temp', 'wetb', 'dewpt', 'vappr', 'rhum', 'msl', 'wdsp', 'wddir', 
                    'sun', 'avg_rain_per_interval', 'DATE', 'HOUR', 'MINUTE']
    return bike_station.merge(exp_weather_df[weather_cols], on=['DATE', 'HOUR', 'MINUTE'])



