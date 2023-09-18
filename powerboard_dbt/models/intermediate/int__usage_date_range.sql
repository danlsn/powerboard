{{config(enabled=False)}}

with
    min_max_date as (
        select
            min(date) as min_date,
            max(date) as max_date
        from {{ ref('stg_main__usage') }}
                    ),

    final as (
        select *
        from min_max_date
             )

select *
from final
