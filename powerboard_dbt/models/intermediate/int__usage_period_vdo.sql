with
    vdo as (
        select
            effective_from,
            effective_to,
            (supply_charge_cents_e4 / 100)::number(10,4) as vdo_supply_charge,
            (usage_charge_kwh_e4 / 100)::number(4,2) as vdo_per_kwh
        from {{ ref('stg_main__vdo') }}
             ),

    usage_periods as (
        select *
        from {{ ref('int__usage_periods') }}
           ),

    joined as (
        select
            up.*,
            (vdo.vdo_supply_charge / 48)::number(10,4) as vdo_supply_charge_per_half_hour,
            vdo.vdo_per_kwh
        from usage_periods as up
        left join vdo
            on up.date_minute between vdo.effective_from and vdo.effective_to
        where vdo.vdo_per_kwh is not null
              ),

    final as (
        select
            *
        from joined
             )

select *
from final
