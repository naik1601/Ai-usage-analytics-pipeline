with unpacked as (

    select
        raw.payload:provider::varchar as provider,
        raw.payload:attempt_id::varchar as attempt_id,
        raw.payload:airflow_run_id::varchar as airflow_run_id,

        to_timestamp_ntz(
            bucket.value:start_time::number,
            0
        )::date as usage_date,

        result.value:model::varchar as model,
        result.value:project_id::varchar as project_id,
        result.value:api_key_id::varchar as api_key_id,

        result.value:input_tokens::number(38, 0)
            as input_tokens,

        result.value:output_tokens::number(38, 0)
            as output_tokens,

        result.value:num_model_requests::number(38, 0)
            as model_requests,

        raw.source_file,
        raw.loaded_at

    from {{ source('bronze', 'openai_usage_raw') }} as raw,

        lateral flatten(
            input => raw.payload:response:data
        ) as bucket,

        lateral flatten(
            input => bucket.value:results
        ) as result

),

current_records as (

    select u.*

    from unpacked as u

    inner join {{ ref('latest_completed_days') }} as latest
        on u.provider = latest.provider
        and latest.dataset = 'completions_usage'
        and u.usage_date = latest.usage_date
        and u.attempt_id = latest.attempt_id
        and u.airflow_run_id = latest.airflow_run_id

)

select
    sha2(
        to_json(
            array_construct(
                provider,
                usage_date,
                model,
                project_id,
                api_key_id
            )
        ),
        256
    ) as usage_id,

    provider,
    usage_date,
    model,
    project_id,
    api_key_id,
    input_tokens,
    output_tokens,
    input_tokens + output_tokens as total_tokens,
    model_requests,
    attempt_id,
    source_file,
    loaded_at

from current_records