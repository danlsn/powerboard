{% set sql_statement %}
    select
        min(effective_from) as min_date,
        max(effective_to) as max_date
    from {{ ref('int__credits_against_usage') }}
{% endset %}

{%- set date_range = dbt_utils.get_query_results_as_dict(sql_statement) -%}
{%- set min_date = date_range["MIN_DATE"][0] -%}
{%- set max_date = date_range["MAX_DATE"][0] -%}
{%- set date_range = dbt_date.get_base_dates(start_date=min_date, end_date=max_date) -%}

with
    sites as (select * from {{ ref("stg_main__sites") }}),

    usage_periods as (
        {{
            dbt_date.get_base_dates(
                start_date=min_date, end_date=max_date, datepart="day"
            )
        }}
    ),

    site_periods as (
        select s.*, up.date_day::date as date_day
        from sites as s
        cross join usage_periods as up
    ),

    usage_credits as (select * from {{ ref("int__credits_against_usage") }}),

    joined as (
        select
            sp.*,
            uc.amount_cents_per_kwh as credits_per_kwh
        from site_periods as sp
        join usage_credits as uc
            on sp.nmi = uc.nmi
            and sp.date_day between uc.effective_from and uc.effective_to
        where uc.amount_cents_per_kwh is not null
    ),

    final as (
        select
            "ID",
            "NMI",
            "ACCOUNT_NUMBER",
            "DATE_DAY",
            sum("CREDITS_PER_KWH") as "CREDITS_PER_KWH"
        from joined
        group by all
        )

select *
from final
