with
    sites as (
        select *
        from {{ ref('stg_main__sites') }}
             ),

    usage as (
        select
            u.usage_date,
            u.nmi,
            u.site_id,
            s.name,
            s.account_number,
            u.kwh,
            u.cost,
            u.cost_adjusted,
            u.cost_vdo
        from {{ ref('int__usage_fee_credit_adjusted') }} as u
        left join sites as s on u.site_id = s.id
        where usage_date between '2022-07-01' and '2023-06-30'
             ),

    final as (
        select
            account_number,
            name,
            nmi,
            site_id,
            usage_date,
            sum(kwh) as kwh,
            sum(cost) as cost,
            sum(cost_adjusted) as cost_adjusted,
            sum(cost_vdo) as cost_vdo,
            (sum(cost) / sum(kwh))::number(10,4) as cost_per_kwh
        from usage
        group by all
             )

select *
from final
