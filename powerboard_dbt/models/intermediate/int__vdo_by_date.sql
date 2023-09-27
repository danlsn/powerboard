{% set sql_statement %}
    select
        min(effective_from) as MIN_DATE,
        max(effective_to) as MAX_DATE
    from {{ ref('base_main__vdo') }}
{% endset %}

{%- set date_range = dbt_utils.get_query_results_as_dict(sql_statement) -%}
{%- set min_date = date_range["MIN_DATE"][0] -%}
{%- set max_date = date_range["MAX_DATE"][0] -%}


with
    base_dates as (
        {{ dbt_date.get_base_dates(start_date=min_date, end_date=max_date) }}
    ),

    vdo as (
        select *
        from {{ ref('base_main__vdo') }}
             ),

    final as (
        select
            b.date_day,
            v.distribution_zone,
            v.supply_charge_cents_e4,
            v.usage_charge_structure,
            v.usage_charge_kwh_e4,
            v.controlled_load_kwh_e4
        from base_dates as b
        join vdo as v
            on b.date_day between v.effective_from and v.effective_to
             )

select *
from final

