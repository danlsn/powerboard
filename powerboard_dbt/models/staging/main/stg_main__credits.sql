with source as (

    select * from {{ source('amber', 'credits') }}

),

renamed as (

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

    from source

)

select * from renamed

