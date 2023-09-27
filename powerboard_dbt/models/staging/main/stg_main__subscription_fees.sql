with source as (

    select * from {{ source('main','load_amber__subscription_fees') }}

),

renamed as (

    select
        fee_type,
        monthly_fee,
        effective_from,
        effective_to

    from source

)

select * from renamed

