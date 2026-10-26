# Custom Airflow image: official image + this project's Python dependencies.
ARG AIRFLOW_VERSION=3.1.0
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

COPY requirements.txt /requirements.txt
# Pinning apache-airflow stops pip from silently upgrading/downgrading Airflow.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
