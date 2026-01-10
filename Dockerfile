FROM apache/airflow:2.10.4-python3.11

USER root

# Instalar Java 17 y procps (para comando ps)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurar Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Los JARs de Hadoop AWS se montan como volumen desde docker-compose.yaml
# Ruta: /opt/airflow/payment_processor/jars/ (montado desde host)

USER airflow

# Instalar dependencias Python
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    pandas==2.1.4 \
    boto3==1.35.0 \
    apache-airflow-providers-amazon==8.28.0