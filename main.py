import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION="sqlite:///my_played_tracks.sqlite"
USER_ID="Fabiuc"
TOKEN="BQB3XCKHjBysf7oUIuJvbkn4zLwFg7_JZfXU6bjIPsl7XL1uX4T40Io68A-KbyKt_dQg6lo47CzioqYe1xZYyY23bjM9z4hi-D4diDyv4RwJBjnn_pBzDVO8B5TbU0MaDHWV3DIPP3WM6jcQj_qwzAhmJgLECXLzeh9rYa7HnI9jezKzk9ZXjEnhUpbqFJG3FkUYoDyAVsM9CvbDnWErIB59FVrDsdyivMlyIwVPfebOxJ7uNutbDpQDnQMpb64ZTXyiG3AD-fU2dXffS4T99KDTY_yUULnIGZyED8MWKcZliNFxBw-7s0eKEaOB1OW3W7erAM4dibahjg"

if __name__ == "__main__":
    headers={
        "Accept":"application/json",
        "Content-Type":"application/json",
        "Authorization":"Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    print(yesterday)
    yesterday_unix_timestamp= int(yesterday.timestamp()) * 1000
    print(yesterday_unix_timestamp)

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    print(data)
