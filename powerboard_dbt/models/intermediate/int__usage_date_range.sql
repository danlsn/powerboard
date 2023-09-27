{{config(enabled=True)}}

with
    min_max_date as (
        select
            min(usage_date) as min_date,
            max(usage_date) as max_date
        from {{ ref('int__usage_adjusted') }}
                    ),

    final as (
        select *
        from min_max_date
             )

select *
from final
