import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import sqlite3
import environ
import time

# Initialise environment variables
env = environ.Env()
environ.Env.read_env()

DATABASE_LOCATION=env('DATABASE_LOCATION')
USER_ID=env('USER_ID')
TOKEN=env('TOKEN')

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
        if not now-timedelta(hours=24) <= timestamp <= now:
            print(timestamp)
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True


if __name__ == "__main__":

    # Extract part of the ETL process

    headers={
        "Accept":"application/json",
        "Content-Type":"application/json",
        "Authorization":"Bearer {token}".format(token=TOKEN)
    }

    # Convert time to Unix timestamp in miliseconds
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_unix_timestamp = #tocheck
    print(yesterday_unix_timestamp)

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}", headers = headers)

    data = r.json()

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
    print(song_df)

    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")
