#!/bin/bash

# Скрипт для запуска ETL пайплайна

echo "🚀 Starting BionicPRO ETL Pipeline Deployment..."

# Проверка Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Создание необходимых директорий
echo "📁 Creating required directories..."
mkdir -p airflow/dags airflow/logs airflow/plugins airflow/config
mkdir -p olap-db

# Копирование конфигурационных файлов
echo "📋 Copying configuration files..."
cp -n docker-compose.airflow.yml docker-compose.yml 2>/dev/null || true

# Запуск сервисов
echo "🐳 Starting Docker services..."
docker-compose down 2>/dev/null
docker-compose up -d

# Ожидание готовности сервисов
echo "⏳ Waiting for services to be ready..."

# Ожидание PostgreSQL
echo "  Waiting for OLAP database..."
until docker exec bionicpro-olap-db pg_isready -U olap_user -d olap_db > /dev/null 2>&1; do
    sleep 2
done

echo "  Waiting for Airflow database..."
until docker exec bionicpro-airflow-db pg_isready -U airflow -d airflow > /dev/null 2>&1; do
    sleep 2
done

# Ожидание Airflow webserver
echo "  Waiting for Airflow webserver..."
until curl -s http://localhost:8081/health > /dev/null 2>&1; do
    sleep 5
done

# Инициализация Airflow
echo "🔧 Initializing Airflow..."
docker exec bionicpro-airflow-webserver airflow db upgrade
docker exec bionicpro-airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@bionicpro.com

# Запуск DAG
echo "⚡ Enabling ETL DAG..."
docker exec bionicpro-airflow-webserver airflow dags unpause crm_to_olap_etl_pipeline

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "🌐 Access URLs:"
echo "   Airflow UI:     http://localhost:8081 (admin/admin)"
echo "   OLAP Database:  localhost:5434 (olap_user/olap_password)"
echo ""
echo "📊 To trigger the ETL pipeline manually:"
echo "   1. Go to http://localhost:8081"
echo "   2. Find 'crm_to_olap_etl_pipeline' DAG"
echo "   3. Click 'Trigger DAG' button"
echo ""
echo "📋 To view logs:"
echo "   docker logs bionicpro-airflow-webserver"
echo "   docker logs bionicpro-airflow-scheduler"
echo ""
echo "🛑 To stop services:"
echo "   docker-compose down"