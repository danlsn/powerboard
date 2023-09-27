{{ config(enabled=True) }}

with
    fees as (
        select
            fee_type,
            monthly_fee,
            (monthly_fee * 100)::number(10,4) as monthly_fee_cents,
            effective_from,
            effective_to,
            discount,
            (discount * 100)::number(10,4) as discount_cents,
            coalesce(monthly_fee_cents - discount_cents, monthly_fee_cents) as monthly_net_fee_cents
        from {{ source('main','load_amber__subscription_fees') }}
    ),

    credits as (
        select
            nmi,
            account_number,
            effective_from,
            effective_to,
            date_applied,
            invoice_applied,
            type,
            description,
            amount_cents
        from {{ source('main','load_amber__credits') }}
            ),

    final as (
        select *
        from fees
    )

select *
from final

