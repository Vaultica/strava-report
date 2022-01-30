#!/usr/bin/env python3

from requests import request
from stravalib.client import Client
import pickle
import pandas as pd
import time

from pandas.io import gbq
from google.cloud import bigquery
from google_auth_oauthlib import flow

from strava_config import MY_STRAVA_CLIENT_ID, MY_STRAVA_CLIENT_SECRET

PROJECT_ID      = 'secret-compass-181513'
LAST_ACTIVITIES = 30

my_cols =['id',
          'name',
          'average_speed',
          'average_heartrate',
          'average_watts',
          'distance',
          'elapsed_time',
          'total_elevation_gain',
          'type',
          'start_date_local',
          'kudos_count',
          'gear_id',
          'average_temp',
          'moving_time',
          'photo_count',
          'athlete_count',
          'comment_count',
          'description',
          'location_country',
          'timezone',
          'start_latlng',
          'upload_id'
          ]

client = Client()

with open('access_token.pickle', 'rb') as f:
    access_token = pickle.load(f)

print(f'Latest access token read from file: {access_token}')

if time.time() > access_token['expires_at']:
    print('Token has expired, will refresh')
    refresh_response = client.refresh_access_token(client_id=MY_STRAVA_CLIENT_ID, client_secret=MY_STRAVA_CLIENT_SECRET, refresh_token=access_token['refresh_token'])
    access_token = refresh_response
    with open('access_token.pickle', 'wb') as f:
        pickle.dump(refresh_response, f)
    print('Refreshed token saved to file')
    client.access_token = refresh_response['access_token']
    client.refresh_token = refresh_response['refresh_token']
    client.token_expires_at = refresh_response['expires_at']

else:
    print('Token still valid, expires at {}'
          .format(time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.localtime(access_token['expires_at']))))
    client.access_token = access_token['access_token']
    client.refresh_token = access_token['refresh_token']
    client.token_expires_at = access_token['expires_at']

print(f'Fetching the last {LAST_ACTIVITIES} activities from Strava')
activities = client.get_activities(limit=LAST_ACTIVITIES)

data = []

for activity in activities:
    my_dict = activity.to_dict()
    data.append([my_dict.get(x) for x in my_cols])

print(f'Saving activities as Excel file')
df = pd.DataFrame(data, columns=my_cols)
df.to_excel('strava_activities.xlsx')

print(f'Loading activities to BigQuery')
gbq.to_gbq(df, 'strava.new_activities', PROJECT_ID, if_exists='replace')
