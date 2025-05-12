FROM apache/airflow:2.10.0

USER root
COPY ./requirements_docker.txt /requirements_docker.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements_docker.txt