import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = 'sqlite://my_played_tracks.sqlite'
USER_ID = '31bkuggdupa7tuulnzslhhd4xmpe'
TOKEN = 'BQDBfugYOJmWJZQTdlqldS_G1SXNguaTk6gUWcrcVj2MSYsYtDd08RiANHNTcPSqkBGPavMDyLazEKpLUHRo7gxq8HzUs3HtJzeRt-GgecOwGeLerKBah1ENVNBKtQDGzV6dwhvFV2HeFhOnYWweGtAhM2Uqr_ZSP_bvp00LzPCLxK6duG20TxZOFCeOmJT7ZRxNEyR0sPAwXQ'

if __name__ == '__main__':

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    response = requests.get('https://api.spotify.com/v1/me/player/recently-played', headers=headers)

    data = response.json()

    # print(data)
    # print(response.status_code)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])
    
    song_dict = {
        'song_name': song_names,
        'artist_name': artist_names,
        'played_at': played_at_list,
        'timestamp': timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=['song_name', 'artist_name', 'played_at', 'timestamp'])

    print(song_df)