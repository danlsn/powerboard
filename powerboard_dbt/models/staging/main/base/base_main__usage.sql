with
    source as (select * from {{ source("main", "load_amber__usage") }}),

    renamed as (

        select
            "date" as usage_date,
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
            kwh::number(10,3) as kwh,
            per_kwh::number(10,5) as per_kwh,
            cost::number(10,4) as cost,
            (renewables::number(7,4) / 100)::number(5,4) as renewables,
            spot_per_kwh::number(10,5) as spot_per_kwh

        from source as s
    ),

    final as (select * from renamed)

select *
from final
