{{ config(materialized='incremental',
          unique_key='id',
          incremental_strategy='insert_overwrite',
          partition_by={
             "field": "start_date",
             "data_type": "datetime",
             "granularity": "day"
          },
          enabled=True)
}}

with source_data as (
  select distinct
    id,
    name,
    round(average_speed*3.6, 1)     as avg_speed,
    average_speed,
    average_heartrate,
    average_watts,
    round(distance/1000, 1)    as distance_km,
    elapsed_time,
    justify_interval((make_interval(second => elapsed_time))) elapsed_time_interval,
    safe.parse_datetime('%Y-%m-%dT%H:%M:%S', start_date_local) as start_date,
    EXTRACT(year from safe.parse_datetime('%Y-%m-%dT%H:%M:%S', start_date_local)) as start_year,
    EXTRACT(month from safe.parse_datetime('%Y-%m-%dT%H:%M:%S', start_date_local)) as start_month,
    EXTRACT(dayofweek from safe.parse_datetime('%Y-%m-%dT%H:%M:%S', start_date_local)) as start_day,
    start_date_local,
    round(total_elevation_gain, 0) as total_elevation_gain,
    type,
    kudos_count,
    gear_id,
    average_temp,
    moving_time,
    justify_interval((make_interval(second => moving_time))) moving_time_interval,
    photo_count,
    athlete_count,
    comment_count,
    description
  from {{ source('strava', 'new_activities') }} activities
  where id not in (select id from {{this}})
)

select
     activities.*,
     gear.bike
from source_data as activities
left join {{ ref('strava_gear') }} gear
     on activities.gear_id = gear.id
