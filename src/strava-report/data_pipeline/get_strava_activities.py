#!/usr/bin/env python3

import argparse
import math
import pickle
import time
import pandas as pd
from pandas.io import gbq
import requests
from stravalib.client import Client

from strava_config import MY_STRAVA_CLIENT_ID, MY_STRAVA_CLIENT_SECRET

# --- CONFIGURATION & CONSTANTS ---
PROJECT_ID = 'secret-compass-181513'
DESTINATION_TABLE = 'strava.new_activities'
TOKEN_FILE = 'access_token.pickle'
EXCEL_BACKUP_PATH = 'strava_activities.xlsx'

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


def get_valid_token(pickle_path, client_id, client_secret):
  """Read, check expiration, and optionally refresh the Strava access token."""
  client = Client()

  with open(pickle_path, 'rb') as f:
    token_data = pickle.load(f)

  print(f'Latest access token read from file: {token_data}')

  if time.time() > token_data['expires_at']:
    print('Token has expired, will refresh')
    refresh_response = client.refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=token_data['refresh_token'],
    )
    with open(pickle_path, 'wb') as f:
      pickle.dump(refresh_response, f)
    print('Refreshed token saved to file')
    return refresh_response['access_token']
  else:
    expires_str = time.strftime(
        '%a, %d %b %Y %H:%M:%S %Z', time.localtime(token_data['expires_at'])
    )
    print(f'Token still valid, expires at {expires_str}')
    return token_data['access_token']


def fetch_activities(token, num_activities=30, activity_id=None):
  """Extract activity data directly from the Strava REST API."""
  headers = {'Authorization': f'Bearer {token}'}

  if activity_id:
    print(
        f'Fetching single activity {activity_id} directly from Strava REST API'
    )
    url = f'https://www.strava.com/api/v3/activities/{activity_id}'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return [response.json()]

  print(
      f'Fetching the last {num_activities} activities directly from Strava REST'
      ' API'
  )
  activities = []
  page_size = 200  # Strava endpoint limit per page
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
  """Transform raw JSON response into a cleaned DataFrame matching warehouse schema."""
  data = []
  for activity in activities_raw:
    row = []
    for col in selected_columns:
      val = activity.get(col, None)

      # Convert coordinate list to a comma-separated string for BigQuery
      if col == 'start_latlng' and isinstance(val, list):
        val = ', '.join(map(str, val)) if val else None

      row.append(val)
    data.append(row)

  return pd.DataFrame(data, columns=selected_columns)


def save_local_backup(df, filepath=EXCEL_BACKUP_PATH):
  """Save DataFrame locally to an Excel file."""
  print(f'Saving activities to {filepath}')
  df.to_excel(filepath, index=False)


def load_to_bigquery(
    df, project_id=PROJECT_ID, table=DESTINATION_TABLE, schema=TABLE_SCHEMA
):
  """Append DataFrame records to the target BigQuery table."""
  print(f'Loading {len(df)} activities to BigQuery ({table})')
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
  token = get_valid_token(TOKEN_FILE, MY_STRAVA_CLIENT_ID, MY_STRAVA_CLIENT_SECRET)
  raw_activities = fetch_activities(
      token, num_activities=args.num_activities, activity_id=args.activity_id
  )

  if not raw_activities:
    print('No activities returned from API.')
    return

  df = transform_activities(raw_activities, MY_COLS)
  save_local_backup(df, EXCEL_BACKUP_PATH)
  load_to_bigquery(df, PROJECT_ID, DESTINATION_TABLE, TABLE_SCHEMA)


if __name__ == '__main__':
  main()
