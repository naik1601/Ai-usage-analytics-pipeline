{{ config(materialized='table', schema='GOLD') }}

with monthly as (

    select
        provider,
        date_trunc('month', usage_date)::date as usage_month,

        count(*) as covered_days,
        sum(usage_loaded) as usage_loaded_days,
        sum(costs_loaded) as costs_loaded_days,

        sum(input_tokens) as input_tokens,
        sum(output_tokens) as output_tokens,
        sum(total_tokens) as total_tokens,
        sum(model_requests) as model_requests,
        sum(reported_cost_usd) as reported_cost_usd

    from {{ ref('gold_daily_summary') }}

    group by
        provider,
        date_trunc('month', usage_date)::date

)

select
    sha2(
        to_json(array_construct(provider, usage_month)),
        256
    ) as monthly_id,

    provider,
    usage_month,
    day(last_day(usage_month)) as calendar_days,
    covered_days,
    usage_loaded_days,
    costs_loaded_days,
    input_tokens,
    output_tokens,
    total_tokens,
    model_requests,
    reported_cost_usd

from monthly