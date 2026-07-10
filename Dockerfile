FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    dbt-bigquery==1.7.0 \
    google-cloud-bigquery \
    google-cloud-storage \
    pandas \
    pandas-gbq \
    requests \
    python-dotenv