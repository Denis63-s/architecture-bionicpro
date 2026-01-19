# Используем официальный образ Apache Airflow
FROM apache/airflow:2.1.2

# Устанавливаем рабочую директорию
WORKDIR /opt/airflow

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем DAG файлы в контейнер
COPY dags/ /opt/airflow/dags/

# Настроим, чтобы Airflow был готов к запуску
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
