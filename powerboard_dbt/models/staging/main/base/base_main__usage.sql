with
    source as (select * from {{ source("amber", "usage") }}),
    renamed as (

        select
            date,
            nem_time_start,
            nem_time,
            start_time as start_time_utc,
            end_time as end_time_utc,
            site_id,
            channel_type,
            channel_identifier,
            type,
            duration,
            descriptor,
            spike_status,
            quality,
            kwh,
            per_kwh,
            cost,
            renewables,
            spot_per_kwh

        from source
    ),

    add_cols as (
        select *, date_part(['year', 'month', 'day', 'hour', 'minute'], nem_time_start) as date_time
        from renamed
    ),

    final as (select * from add_cols)

select *
from renamed
