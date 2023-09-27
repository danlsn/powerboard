with source as (

     select * from {{ source('main','load__vdo') }}

),

renamed as (

select
    distribution_zone,
    supply_charge_cents_e4,
    usage_charge_structure,
    usage_charge_kwh_e4,
    controlled_load_kwh_e4,
    effective_from,
    effective_to

from source

    )

select * from renamed

