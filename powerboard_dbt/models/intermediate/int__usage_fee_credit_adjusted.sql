with
    fee_adjusted_usage as (
        select *
        from {{ ref('int__usage_adjusted') }}
                          ),

    credits_per_kwh as (
        select *
        from {{ ref('int__site_credits_per_kwh') }}
                       ),

    vdo as (
           select
               effective_from,
               effective_to,
               (supply_charge_cents_e4 / 100)::number(10,4) as vdo_supply_charge,
               (usage_charge_kwh_e4 / 100)::number(4,2) as vdo_per_kwh
           from {{ ref('stg_main__vdo') }}
           ),

    joined as (
        select
            fau.*,
            coalesce(cpk.credits_per_kwh, 0) as credits_per_kwh,
            (vdo.vdo_supply_charge / 48)::number(10,4) as vdo_supply_charge_per_half_hour,
            vdo.vdo_per_kwh,
            (vdo.vdo_per_kwh * fau.kwh) as cost_vdo
        from fee_adjusted_usage as fau
        left join credits_per_kwh as cpk
            on fau.nmi = cpk.nmi
            and fau.usage_date = cpk.date_day
        left join vdo
            on fau.usage_date between vdo.effective_from and vdo.effective_to
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(['site_id', 'nem_time']) }} as site_nem_time_id,
            *,
            (cost + fee_per_half_hour - (credits_per_kwh * kwh))::number(10,4) as cost_adjusted,
            cost_adjusted - cost as cost_adjustment,
            cast( {{ dbt_utils.safe_divide('cost_adjusted', 'kwh') }} as number(10,4)) as per_kwh_adjusted,
            cost_vdo - cost_adjusted as cost_vdo_savings
        from joined
             )

select *
from final
