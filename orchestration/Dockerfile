FROM apache/airflow:3.1.7-python3.12

RUN pip install --no-cache-dir \
    "apache-airflow==3.1.7" \
    apache-airflow-providers-amazon \
    apache-airflow-providers-standard \
    apache-airflow-providers-snowflake \
    requests \
    --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt"
    
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir \
       "dbt-core==1.12.7" \
       "dbt-snowflake==1.12.5"