{{config(enabled=False)}}

{% set sql_statement %}
    select
        min_date,
        max_date
    from {{ ref('int__usage_date_range') }}
{% endset %}

{%- set date_range = dbt_utils.get_query_results_as_dict(sql_statement) -%}
{%- set min_date = date_range["min_date"][0] -%}
{%- set max_date = date_range["max_date"][0] -%}

with
    usage_periods as (
        select cast(range as timestamp) as period
        from range(date '{{ min_date }}', date '{{ max_date }}', interval 30 minute)
    ),

    final as (
        select
            row_number() over (order by period) as period_id,
            period as period_start,
            cast(period as date) as date
        from usage_periods
        )

select *
from final
