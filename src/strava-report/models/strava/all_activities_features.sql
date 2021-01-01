{{ config(materialized='view', enabled=True) }}

with source_data as (

  select
    name,
    round(average_speed*3.6, 1)     as avg_speed,
    average_heartrate,
    average_watts,
    round(distance/100, 1)    as distance_km,
    elapsed_time,
    total_elevation_gain,
    type,
    start_date_local,
    kudos_count,
    gear.bike                 as bike,
    average_temp,
    moving_time,
    photo_count,
    athlete_count,
    comment_count,
    description
  from {{ source('strava', 'all_activities') }} activities
  left join {{ ref('strava_gear') }} gear
       on activities.gear_id = gear.id

)

select
     *
from source_data
