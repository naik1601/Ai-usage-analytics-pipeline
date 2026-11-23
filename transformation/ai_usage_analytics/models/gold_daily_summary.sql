{{ config(materialized='table', schema='GOLD') }}

with coverage as (

    select
        provider,
        usage_date,

        max(
            case when dataset = 'completions_usage'
                then 1 else 0 end
        ) as usage_loaded,

        max(
            case when dataset = 'costs'
                then 1 else 0 end
        ) as costs_loaded

    from {{ ref('latest_completed_days') }}

    group by provider, usage_date

),

usage_totals as (

    select
        provider,
        usage_date,
        sum(input_tokens) as input_tokens,
        sum(output_tokens) as output_tokens,
        sum(total_tokens) as total_tokens,
        sum(model_requests) as model_requests

    from {{ ref('silver_openai_usage') }}

    group by provider, usage_date

),

cost_totals as (

    select
        provider,
        usage_date,
        sum(cost_amount) as reported_cost_usd

    from {{ ref('silver_openai_costs') }}

    where upper(currency) = 'USD'

    group by provider, usage_date

)

select
    sha2(
        to_json(array_construct(c.provider, c.usage_date)),
        256
    ) as daily_id,

    c.provider,
    c.usage_date,
    c.usage_loaded,
    c.costs_loaded,

    case when c.usage_loaded = 1
        then coalesce(u.input_tokens, 0)
    end as input_tokens,

    case when c.usage_loaded = 1
        then coalesce(u.output_tokens, 0)
    end as output_tokens,

    case when c.usage_loaded = 1
        then coalesce(u.total_tokens, 0)
    end as total_tokens,

    case when c.usage_loaded = 1
        then coalesce(u.model_requests, 0)
    end as model_requests,

    case when c.costs_loaded = 1
        then coalesce(s.reported_cost_usd, 0)
    end as reported_cost_usd

from coverage as c

left join usage_totals as u
    on c.provider = u.provider
    and c.usage_date = u.usage_date

left join cost_totals as s
    on c.provider = s.provider
    and c.usage_date = s.usage_date