with
    usage as (select * from {{ ref("base_main__usage") }}),

    sites as (select * from {{ ref("base_main__sites") }}),

    fees as (select * from {{ ref("base_main__subscription_fees") }}),

    monthly_fees as (
        select distinct
            date_part('month', u.usage_date) as month,
            date_trunc('month', u.usage_date) as month_start,
            last_day(u.usage_date) as month_end,
            datediff('day', date_trunc('month', u.usage_date), last_day(u.usage_date))
            + 1 as days_in_month,
            f.monthly_net_fee_cents as monthly_fee_cents
        from usage as u
        left join fees as f on u.usage_date between f.effective_from and f.effective_to
    ),

    half_hourly_fees as (
        select
            *,
            (monthly_fee_cents / days_in_month)::number(10,4) as fee_per_day,
            (monthly_fee_cents / days_in_month / 48)::number(10,4) as fee_per_half_hour
        from monthly_fees
    ),

    joined as (
        select
            {{ dbt_utils.star(from=ref("base_main__usage", relation_alias="u")) }},
            s.name as site_name,
            s.supply_address,
            s.account_number,
            s.nmi,
            s.active_from as site_active_from,
            f.monthly_fee_cents,
            f.days_in_month,
            f.fee_per_day,
            f.fee_per_half_hour
            -- (u.cost + f.fee_per_half_hour)::number(8,4) as cost_adjusted
        from usage u
        left join sites s on u.site_id = s.id
        left join
            half_hourly_fees as f on u.usage_date between f.month_start and f.month_end
    ),

    adjusted as (
        select
            j.*
            -- (j.cost_adjusted / j.kwh)::number(10,4) as per_kwh_adjusted
        from joined as j
    ),

    renewables as (
        select
            *,
            (kwh * renewables)::number(10,3) as kwh_renewables,
            (kwh * (1 - renewables))::number(10,3) as kwh_non_renewables
        from adjusted
    ),

    final as (select distinct * from renewables)

select *
from final
