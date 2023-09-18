with source as (

    select * from {{ source('amber', 'subscription_fees') }}

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

