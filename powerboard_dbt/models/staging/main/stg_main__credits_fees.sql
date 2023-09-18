with
    fees as (
        select
            fee_type,
            monthly_fee,
            effective_from,
            effective_to
        from {{ source('amber', 'subscription_fees') }}
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
        from {{ source('amber', 'credits') }}
            ),

