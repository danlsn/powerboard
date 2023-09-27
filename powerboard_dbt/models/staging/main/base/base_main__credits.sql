with source as (

    select * from {{ source('main','load_amber__credits') }}

),

renamed as (

    select
        nmi::int as nmi,
        account_number,
        effective_from,
        effective_to,
        datediff('day', effective_from, effective_to) + 1 as days_in_period,
        date_applied,
        invoice_applied,
        type,
        description,
        amount_cents

    from source

)

select * from renamed

