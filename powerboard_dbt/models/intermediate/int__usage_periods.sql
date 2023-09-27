{% set sql_statement %}
select
    min_date,
    max_date
from {{ ref('int__usage_date_range') }}
    {% endset %}

{%- set date_range = dbt_utils.get_query_results_as_dict(sql_statement) -%}
    {%- set min_date = date_range["MIN_DATE"][0] -%}
    {%- set max_date = date_range["MAX_DATE"][0] -%}
    {%- set date_range = dbt_date.get_base_dates(start_date=min_date, end_date=max_date) -%}

with
    usage_periods as (
        {{ dbt_date.get_base_dates(start_date=min_date, end_date=max_date, datepart="minute") }}
    ),

    filtered as (
        select *
        from usage_periods
        where date_part('minute', date_minute) in (0, 30)
    ),

    final as (
        select *
        from filtered
    )

select *
from final
