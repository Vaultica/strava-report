#!/usr/bin/env python3

import argparse
import json
import math
import time
from google.cloud import secretmanager
import pandas as pd
from pandas.io import gbq
import requests
from stravalib.client import Client

# --- CONFIGURATION & CONSTANTS ---
PROJECT_ID = 'secret-compass-181513'
DESTINATION_TABLE = 'strava.new_activities'

MY_COLS = [
    'id',
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
    'upload_id',
]

TABLE_SCHEMA = [
    {'name': 'id', 'type': 'INTEGER'},
    {'name': 'name', 'type': 'STRING'},
    {'name': 'average_speed', 'type': 'FLOAT'},
    {'name': 'average_heartrate', 'type': 'FLOAT'},
    {'name': 'average_watts', 'type': 'FLOAT'},
    {'name': 'distance', 'type': 'FLOAT'},
    {'name': 'elapsed_time', 'type': 'INTEGER'},
    {'name': 'total_elevation_gain', 'type': 'FLOAT'},
    {'name': 'type', 'type': 'STRING'},
    {'name': 'start_date_local', 'type': 'STRING'},
    {'name': 'kudos_count', 'type': 'INTEGER'},
    {'name': 'gear_id', 'type': 'STRING'},
    {'name': 'average_temp', 'type': 'FLOAT'},
    {'name': 'moving_time', 'type': 'INTEGER'},
    {'name': 'photo_count', 'type': 'INTEGER'},
    {'name': 'athlete_count', 'type': 'INTEGER'},
    {'name': 'comment_count', 'type': 'INTEGER'},
    {'name': 'description', 'type': 'STRING'},
    {'name': 'location_country', 'type': 'STRING'},
    {'name': 'timezone', 'type': 'STRING'},
    {'name': 'start_latlng', 'type': 'STRING'},
    {'name': 'upload_id', 'type': 'INTEGER'},
]


def parse_arguments():
  """Parse command-line arguments."""
  parser = argparse.ArgumentParser(
      description='Fetch Strava activities and load into BigQuery.'
  )
  parser.add_argument(
      '-n',
      '--num-activities',
      type=int,
      default=30,
      help='Number of recent activities to fetch (default: 30)',
  )
  parser.add_argument(
      '-a',
      '--activity-id',
      type=int,
      default=None,
      help='Specific activity ID to fetch (overrides --num-activities)',
  )
  return parser.parse_args()


def get_secret(secret_id, project_id=PROJECT_ID):
  """Access secret payload from GCP Secret Manager."""
  client = secretmanager.SecretManagerServiceClient()
  name = f'projects/{project_id}/secrets/{secret_id}/versions/latest'
  response = client.access_secret_version(request={'name': name})
  return json.loads(response.payload.data.decode('UTF-8'))


def save_secret_version(secret_id, data_dict, project_id=PROJECT_ID):
  """Add a new payload version to GCP Secret Manager."""
  client = secretmanager.SecretManagerServiceClient()
  parent = f'projects/{project_id}/secrets/{secret_id}'
  payload = json.dumps(data_dict).encode('UTF-8')
  client.add_secret_version(
      request={'parent': parent, 'payload': {'data': payload}}
  )
  print(f'New version saved to Secret Manager for {secret_id}')


def get_valid_token():
  """Retrieve and automatically refresh tokens via Secret Manager."""
  creds = get_secret('strava_credentials')
  token_data = get_secret('strava_token_data')

  client = Client()

  # Buffer expiration by 60 seconds
  if time.time() > (token_data['expires_at'] - 60):
    print('Token expired or expiring soon, refreshing...')
    refresh_response = client.refresh_access_token(
        client_id=creds['client_id'],
        client_secret=creds['client_secret'],
        refresh_token=token_data['refresh_token'],
    )
    save_secret_version('strava_token_data', refresh_response)
    return refresh_response['access_token']

  print(
      'Token still valid, expires at'
      f" {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(token_data['expires_at']))}"
  )
  return token_data['access_token']


def fetch_activities(token, num_activities=30, activity_id=None):
  """Extract activity data directly from the Strava REST API."""
  headers = {'Authorization': f'Bearer {token}'}

  if activity_id:
    print(f'Fetching single activity {activity_id}...')
    url = f'https://www.strava.com/api/v3/activities/{activity_id}'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return [response.json()]

  print(f'Fetching the last {num_activities} activities...')
  activities = []
  page_size = 200
  pages_needed = math.ceil(num_activities / page_size)

  for page in range(1, pages_needed + 1):
    remaining = num_activities - len(activities)
    current_per_page = min(page_size, remaining)

    url = 'https://www.strava.com/api/v3/athlete/activities'
    params = {'page': page, 'per_page': current_per_page}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    page_data = response.json()
    if not page_data:
      break
    activities.extend(page_data)

  return activities


def transform_activities(activities_raw, selected_columns):
  """Transform raw JSON response into a cleaned DataFrame matching schema."""
  data = []
  for activity in activities_raw:
    row = []
    for col in selected_columns:
      val = activity.get(col, None)
      if col == 'start_latlng' and isinstance(val, list):
        val = ', '.join(map(str, val)) if val else None
      row.append(val)
    data.append(row)

  return pd.DataFrame(data, columns=selected_columns)


def load_to_bigquery(
    df, project_id=PROJECT_ID, table=DESTINATION_TABLE, schema=TABLE_SCHEMA
):
  """Append DataFrame records to the target BigQuery table."""
  print(f'Loading {len(df)} activities to BigQuery ({table})...')
  gbq.to_gbq(
      df,
      destination_table=table,
      project_id=project_id,
      table_schema=schema,
      if_exists='append',
  )
  print(f'Successfully loaded {len(df)} activities to BigQuery.')


def main():
  args = parse_arguments()
  token = get_valid_token()
  raw_activities = fetch_activities(
      token, num_activities=args.num_activities, activity_id=args.activity_id
  )

  if not raw_activities:
    print('No activities returned.')
    return

  df = transform_activities(raw_activities, MY_COLS)
  load_to_bigquery(df)


if __name__ == '__main__':
  main()
