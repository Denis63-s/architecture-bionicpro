FROM apache/airflow:2.7.1-python3.11

USER root

# Установка системных зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libpq-dev \
        curl \
        vim \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Установка Python зависимостей
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Копирование DAGs и конфигурации
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/
COPY scripts/ ${AIRFLOW_HOME}/scripts/

# Создание необходимых директорий
RUN mkdir -p ${AIRFLOW_HOME}/logs && \
    mkdir -p ${AIRFLOW_HOME}/config

WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080