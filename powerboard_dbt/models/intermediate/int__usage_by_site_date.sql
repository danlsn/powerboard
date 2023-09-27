with
    usage as (
        select *
        from {{ ref('int__usage_adjusted') }}
             ),

    grouped as (
        select
            site_id,
            nmi,
            usage_date,
            sum(kwh) as kwh,
            sum(cost) as cost
        from usage
        group by all
               ),

    final as (
        select
            *,
            (cost / kwh)::number(10,4) as per_kwh
        from grouped
             )

select *
from final
