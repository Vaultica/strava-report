{{ config(materialized='incremental',
          unique_key='id',
          incremental_strategy='insert_overwrite',
          partition_by=
            {
               "field": "start_date",
               "data_type": "datetime",
               "granularity": "day"
            },
          enabled=True)
}}

with raw_source as (
  select
    id,
    name,
    round(average_speed*3.6, 1)                                                        as avg_speed,
    average_speed,
    average_heartrate,
    average_watts,
    round(distance/1000, 1)                                                            as distance_km,
    elapsed_time,
    justify_interval((make_interval(second => elapsed_time)))                          as elapsed_time_interval,

    -- FIXED TIMESTAMP PARSING HOOK:
    -- Safely casts strings (with or without 'Z') to TIMESTAMP, then shifts it directly to a DATETIME object
    case
      when ends_with(start_date_local, 'Z') then datetime(safe_cast(start_date_local as timestamp))
      else safe_cast(start_date_local as datetime)
    end as start_date,

    extract(year from case
      when ends_with(start_date_local, 'Z') then datetime(safe_cast(start_date_local as timestamp))
      else safe_cast(start_date_local as datetime)
    end) as start_year,

    extract(month from case
      when ends_with(start_date_local, 'Z') then datetime(safe_cast(start_date_local as timestamp))
      else safe_cast(start_date_local as datetime)
    end) as start_month,

    extract(dayofweek from case
      when ends_with(start_date_local, 'Z') then datetime(safe_cast(start_date_local as timestamp))
      else safe_cast(start_date_local as datetime)
    end) as start_day,

    start_date_local,
    round(total_elevation_gain, 0) as total_elevation_gain,
    type,
    gear_id,
    average_temp,
    moving_time,
    justify_interval((make_interval(second => moving_time)))                           as moving_time_interval,
    photo_count,
    athlete_count,
    description
  from {{ source('strava', 'new_activities') }} activities

  -- Updated filter constraint
  where (
    safe_cast(start_date_local as datetime) is not null
    or safe_cast(start_date_local as timestamp) is not null
  )
)

, source_data as (
  select * from raw_source

  {% if is_incremental() %}
    where start_date >= datetime_sub(current_datetime(), interval {{ var('lookback_days', 30) }} day)
  {% endif %}

  -- FIX 1: Deduplicate the source data explicitly on id just in case text strings vary
  -- QUALIFY must come absolutely last in the filtering sequence
  qualify row_number() over (partition by id order by start_date_local desc) = 1
)

, gear as (
  select
    id,
    bike
  from {{ ref('strava_gear') }}
  -- Deduplicate old gear_id
  qualify row_number() over (partition by id order by id) = 1 -- Assures 1 row per ID
)

select
     activities.*,
     gear.bike
from source_data as activities
-- some activities don't have a gear
left join gear
     on activities.gear_id = gear.id
