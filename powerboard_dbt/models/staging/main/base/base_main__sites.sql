with source as (

    select * from {{ source('main','load_amber__sites') }}

),

renamed as (

    select
        id,
        name,
        supply_address,
        account_number,
        nmi::int as nmi,
        active_from

    from source

)

select * from renamed

