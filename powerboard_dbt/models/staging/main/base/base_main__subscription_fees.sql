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

),

 fees as (
         select
             fee_type,
             monthly_fee,
             (monthly_fee * 100)::number(10,4) as monthly_fee_cents,
             effective_from,
             effective_to,
             discount,
             (discount * 100)::number(10,4) as discount_cents,
             coalesce(monthly_fee_cents - discount_cents, monthly_fee_cents)::number(10,4) as monthly_net_fee_cents
         from source
         )

select * from fees

