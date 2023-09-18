with source as (

    select * from {{ source('amber', 'credits') }}

),

renamed as (

    select
        nmi,
        account_number,
        effective_from,
        effective_to,
        date_diff('day', effective_from, effective_to) + 1 as days_in_period,
        date_applied,
        invoice_applied,
        type,
        description,
        amount_cents

    from source

)

select * from renamed

