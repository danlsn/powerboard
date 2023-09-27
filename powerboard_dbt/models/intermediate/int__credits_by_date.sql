{% set sql_statement %}
    select
        min(effective_from) as min_date,
        max(effective_to) as max_date
    from {{ ref('stg_main__credits') }}
{% endset %}

{%- set date_range = dbt_utils.get_query_results_as_dict(sql_statement) -%}
{%- set min_date = date_range["MIN_DATE"][0] -%}
{%- set max_date = date_range["MAX_DATE"][0] -%}
{%- set date_range = dbt_date.get_base_dates(start_date=min_date, end_date=max_date) -%}

with
    day_range as (
        {{
            dbt_date.get_base_dates(
                start_date=min_date, end_date=max_date, datepart="day"
            )
        }}
    ),

    credits as (select * from {{ ref("stg_main__credits") }}),

    filtered as (
        select dr.*, c.*
        from day_range as dr
        inner join
            credits as c on dr.date_day between c.effective_from and c.effective_to
    )

select *
from filtered
