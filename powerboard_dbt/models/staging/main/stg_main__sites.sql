with source as (

    select * from {{ source('amber', 'sites') }}

),

renamed as (

    select
        id,
        name,
        supply_address,
        account_number,
        nmi,
        active_from

    from source

)

select * from renamed

