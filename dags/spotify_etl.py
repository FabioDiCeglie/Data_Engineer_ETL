import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv,find_dotenv
import os
import psycopg2
from sqlalchemy import create_engine
import logging

# Initialise environment variables
load_dotenv(find_dotenv())

DATABASE_LOCATION=os.getenv('DATABASE_LOCATION')
USER_ID=os.getenv('USER_ID')
TOKEN=os.getenv('TOKEN')

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Primary Key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    now = datetime.now()
    timestamps = df["played_at"]

    for timestamp in timestamps:
        timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        if now-timedelta(hours=24) >= timestamp >= now:
            print(timestamp)
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True


def run_spotify_etl():
    logging.info("Start DAG")
    # Extract part of the ETL process

    headers={
        "Accept":"application/json",
        "Content-Type":"application/json",
        "Authorization":f"Bearer {TOKEN}"
    }

    # Convert time to Unix timestamp in miliseconds
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    # yesterday_unix_timestamp = int(time.mktime(yesterday.timetuple()))
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    logging.info("before request")

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    try:
        r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}", headers = headers)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    data = r.json()
    logging.info(f"data {data}")

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict,columns=["song_name", "artist_name", "played_at", "timestamp"])

    # Validate
    if check_if_valid_data(song_df):
        logging.info("Data valid, proceed to Load stage")

    # Load

    conn_string = os.getenv("DATABASE_URL")

    db = create_engine(conn_string)
    conn = db.connect()

    try:
        song_df.to_sql("my_played_tracks", con=conn, index=False, if_exists='append')
    except:
        logging.info("Data already exists in the database")

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()

    sql1 = '''SELECT * FROM "public"."my_played_tracks" LIMIT 100'''
    cursor.execute(sql1)
    for i in cursor.fetchall():
        logging.info(f"{i}")

    # conn.commit()
    conn.close()
    logging.info("Close database successfully")
