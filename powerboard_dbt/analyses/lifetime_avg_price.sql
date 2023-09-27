with
    usage as (
        select *
        from {{ ref('int__usage_fee_credit_adjusted') }}
             ),

    aggregated as (
        select
            sum(kwh) as total_kwh,
            sum(cost) as total_cost,
            sum(cost_vdo) as total_cost_vdo,
            sum(cost_adjusted) as total_cost_adjusted
        from usage
                  ),

    final as (
        select
            total_kwh::number(10,2) as total_kwh,
            (total_cost / 100)::number(10,2) as total_cost_$,
            (total_cost_vdo / 100)::number(10,2) as total_cost_vdo_$,
            (total_cost_adjusted / 100)::number(10,2) as total_cost_adjusted_$,
            (total_cost_adjusted - total_cost_vdo)::number(10,2) as total_cost_saved_$,
            (total_cost / total_kwh)::number(10,2) as avg_price
        from aggregated
             )

select *
from final
