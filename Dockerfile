FROM apache/airflow:2.10.4

USER root

#RUN apt-get update && apt-get install -y some-package

USER airflow

COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --user -r ./requirements.txt