with manifests as (

    select
        payload:provider::varchar as provider,
        payload:dataset::varchar as dataset,
        payload:attempt_id::varchar as attempt_id,
        payload:airflow_run_id::varchar as airflow_run_id,

        to_timestamp_tz(
            payload:window_start::varchar
        )::date as window_start,

        to_timestamp_tz(
            payload:window_end::varchar
        )::date as window_end,

        to_timestamp_tz(
            payload:completed_at::varchar
        ) as completed_at

    from {{ source('bronze', 'ingestion_manifests') }}

    where payload:status::varchar = 'complete'
      and payload:provider::varchar = 'openai'

),

covered_days as (

    select
        m.provider,
        m.dataset,
        m.attempt_id,
        m.airflow_run_id,
        m.completed_at,

        dateadd(
            day,
            day_offset.value::integer,
            m.window_start
        )::date as usage_date

    from manifests as m,

        lateral flatten(
            input => array_generate_range(
                0,
                datediff(day, m.window_start, m.window_end)
            )
        ) as day_offset

)

select *
from covered_days

qualify row_number() over (
    partition by provider, dataset, usage_date
    order by completed_at desc, attempt_id desc
) = 1