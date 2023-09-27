with
    usage_periods as (
        select distinct
            usage_date,
            nem_time_start,
            nem_time,
            start_time_utc,
            end_time_utc
        from {{ ref("base_main__usage")}}
                     ),

    final as (
        select *
        from usage_periods
        order by nem_time_start
             )

select *
from final
