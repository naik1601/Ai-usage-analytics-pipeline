import pendulum

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="test_snowflake_connection",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Chicago"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "connection-test"],
) as dag:

    check_login = SQLExecuteQueryOperator(
        task_id="check_login",
        conn_id="snowflake_ai_usage",
        sql="""
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_WAREHOUSE();
        """,
        do_xcom_push=False,
    )

    check_s3_stage = SQLExecuteQueryOperator(
        task_id="check_s3_stage",
        conn_id="snowflake_ai_usage",
        sql="LIST @AI_USAGE_ANALYTICS.BRONZE.AI_USAGE_S3_STAGE;",
        do_xcom_push=False,
    )

    check_login >> check_s3_stage