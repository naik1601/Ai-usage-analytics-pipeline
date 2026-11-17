FROM apache/airflow:3.1.7-python3.12

RUN pip install --no-cache-dir \
    "apache-airflow==3.1.7" \
    apache-airflow-providers-amazon \
    apache-airflow-providers-standard \
    requests \
    --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt"