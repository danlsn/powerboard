with
    credits as (
        select *
        from {{ ref("stg_main__credits") }}
        where type != 'subscription_discount'
        ),

    usage as (select * from {{ ref("int__usage_by_site_date") }}),

    joined as (
        select
            u.SITE_ID,
            u.nmi,
            c.account_number,
            c.effective_from,
            c.effective_to,
            c.type,
            c.description,
            c.amount_cents,
            sum(u.kwh) as kwh_total
        from usage u
        join
            credits c
            on u.nmi = c.nmi
            and u.usage_date between c.effective_from and c.effective_to
        group by all
    ),

    final as (
        select
            *,
            (amount_cents / kwh_total)::number(10,4) as amount_cents_per_kwh
        from joined
             )

select *
from final
