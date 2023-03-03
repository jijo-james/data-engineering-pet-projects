import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = 'sqlite://my_played_tracks.sqlite'
USER_ID = '31bkuggdupa7tuulnzslhhd4xmpe'
TOKEN = 'BQAaH7lsRgdX-4Vkj9ddM5jVlrYAZdNZYbWuz6wySoe3qTmMHohqbzSsNYC3N1Q5RJr8KIgVzEfheixzQpaaYAz8Q4Ln5BVe_xmgRYAf96SR03caEZPkZ9eBzg8fYlnDc9k7Kb4t2tXvEh--3xWZy9V3aJOu2OuacmESn7ivztvH9Qs5TaCf6iH1CZfZKVBkI5vrKklTwGZadg'


def check_if_valid_data(df: pd.DataFrame) -> bool:

    if df.empty:
        print("No songs downloaded. Finish execution")
        return False
    
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")
    
    if df.isnull().values.any():
        raise Exception('Null value found')
    
    return True
    


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

    if check_if_valid_data(song_df):
        print("Data is valid")
        print(song_df.shape[0])