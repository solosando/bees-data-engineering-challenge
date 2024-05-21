FROM apache/airflow:2.7.1-python3.11
# Criar os diretórios necessários
RUN mkdir -p /opt/airflow/dags /opt/airflow/resources

# Copiar os DAGs para o diretório de DAGs do Airflow
COPY dags /opt/airflow/dags

# Copiar o diretório resources para o container
COPY resources /opt/airflow/resources

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark

ENV AWS_ACCESS_KEY_ID=seu_access_key_id
ENV AWS_SECRET_ACCESS_KEY=sua_secret_access_key
