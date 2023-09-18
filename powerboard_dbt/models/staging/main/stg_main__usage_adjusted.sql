with
    usage as (select * from {{ ref("base_main__usage") }}),

    sites as (select * from {{ ref("base_main__sites") }}),

    fees as (select * from {{ ref("base_main__subscription_fees") }}),

    monthly_fees as (
        select distinct
            date_part('month', u.date) as month,
            date_trunc('month', u.date) as month_start,
            last_day(u.date) as month_end,
            date_diff('day', date_trunc('month', u.date), last_day(u.date))
            + 1 as days_in_month,
            f.monthly_fee
        from usage as u
        left join fees as f on u.date between f.effective_from and f.effective_to
    ),

    half_hourly_fees as (
        select
            *,
            monthly_fee / days_in_month as fee_per_day,
            monthly_fee / days_in_month / 48 as fee_per_half_hour
        from monthly_fees
    ),

    joined as (
        select
            u.*,
            s.name as site_name,
            s.supply_address,
            s.account_number,
            s.nmi,
            s.active_from as site_active_from
        from usage u
        left join sites s on u.site_id = s.id
    ),

    final as (select * from joined)

select *
from half_hourly_fees
