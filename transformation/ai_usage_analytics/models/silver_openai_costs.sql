with unpacked as (

    select
        raw.payload:provider::varchar as provider,
        raw.payload:attempt_id::varchar as attempt_id,
        raw.payload:airflow_run_id::varchar as airflow_run_id,

        to_timestamp_ntz(
            bucket.value:start_time::number,
            0
        )::date as usage_date,

        result.value:project_id::varchar as project_id,
        result.value:line_item::varchar as line_item,

        result.value:amount:value::number(38, 12)
            as cost_amount,

        result.value:amount:currency::varchar as currency,

        raw.source_file,
        raw.loaded_at

    from {{ source('bronze', 'openai_costs_raw') }} as raw,

        lateral flatten(
            input => raw.payload:response:data
        ) as bucket,

        lateral flatten(
            input => bucket.value:results
        ) as result

),

current_records as (

    select c.*

    from unpacked as c

    inner join {{ ref('latest_completed_days') }} as latest
        on c.provider = latest.provider
        and latest.dataset = 'costs'
        and c.usage_date = latest.usage_date
        and c.attempt_id = latest.attempt_id
        and c.airflow_run_id = latest.airflow_run_id

)

select
    sha2(
        to_json(
            array_construct(
                provider,
                usage_date,
                project_id,
                line_item,
                currency
            )
        ),
        256
    ) as cost_id,

    provider,
    usage_date,
    project_id,
    line_item,
    cost_amount,
    currency,
    attempt_id,
    source_file,
    loaded_at

from current_records