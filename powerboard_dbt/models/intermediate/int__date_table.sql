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
{%- set date_range = dbt_date.get_base_dates(start_date=min_date, end_date=max_date) -%}

with
    date_dimension as (
        {{ dbt_date.get_date_dimension(start_date=min_date, end_date=max_date) }}
    ),

    -- TODO: Add Financial Year Dimensions
    fin_year as (select * from date_dimension),

    final as (
        select row_number() over (order by date_day) as date_id, * from date_dimension
    )

select *
from final
