"""
ETL DAG для извлечения данных из CRM системы и загрузки в OLAP базу.
Создает витрину данных для отчетов с агрегацией телеметрии по клиентам.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import logging
import json
from typing import Dict, List, Any

# Конфигурация по умолчанию
default_args = {
    'owner': 'bionicpro_team',
    'depends_on_past': False,
    'email': ['admin@bionicpro.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'catchup': False,
}

# Конфигурация DAG
dag = DAG(
    'crm_to_olap_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline из CRM в OLAP с созданием витрины данных',
    schedule_interval='0 2 * * *',  # Ежедневно в 2:00 ночи
    max_active_runs=1,
    tags=['crm', 'olap', 'etl', 'data_mart'],
)

def extract_from_crm(**context) -> Dict[str, List[Dict]]:
    """
    Извлечение данных из CRM системы.
    В реальной реализации здесь будет API запрос к CRM.
    """
    task_instance = context['ti']
    execution_date = context['execution_date']
    
    logging.info(f"Extracting CRM data for date: {execution_date}")
    
    # Имитация данных CRM (в реальности - API запрос)
    crm_customers = [
        {
            'customer_id': 1001,
            'first_name': 'Иван',
            'last_name': 'Иванов',
            'email': 'ivan.ivanov@example.com',
            'company': 'TechCorp',
            'segment': 'premium',
            'registration_date': '2023-01-15',
            'status': 'active',
            'lifetime_value': 15000.50
        },
        {
            'customer_id': 1002,
            'first_name': 'Мария',
            'last_name': 'Петрова',
            'email': 'maria.petrova@example.com',
            'company': 'BusinessSoft',
            'segment': 'enterprise',
            'registration_date': '2023-03-20',
            'status': 'active',
            'lifetime_value': 25000.75
        },
        {
            'customer_id': 1003,
            'first_name': 'Алексей',
            'last_name': 'Сидоров',
            'email': 'alexey.sidorov@example.com',
            'company': 'StartupInc',
            'segment': 'standard',
            'registration_date': '2023-06-10',
            'status': 'active',
            'lifetime_value': 8000.25
        },
        {
            'customer_id': 1004,
            'first_name': 'Елена',
            'last_name': 'Козлова',
            'email': 'elena.kozlova@example.com',
            'company': 'CloudSystems',
            'segment': 'premium',
            'registration_date': '2023-08-05',
            'status': 'active',
            'lifetime_value': 18000.00
        },
        {
            'customer_id': 1005,
            'first_name': 'Дмитрий',
            'last_name': 'Федоров',
            'email': 'dmitry.fedorov@example.com',
            'company': 'DataLabs',
            'segment': 'enterprise',
            'registration_date': '2023-11-30',
            'status': 'active',
            'lifetime_value': 32000.50
        }
    ]
    
    # Имитация данных телеметрии
    telemetry_data = [
        # Клиент 1001
        {'customer_id': 1001, 'date': execution_date.strftime('%Y-%m-%d'), 
         'session_duration_minutes': 45, 'page_views': 120, 'clicks': 85, 'conversions': 2},
        {'customer_id': 1001, 'date': (execution_date - timedelta(days=1)).strftime('%Y-%m-%d'),
         'session_duration_minutes': 38, 'page_views': 95, 'clicks': 70, 'conversions': 1},
        
        # Клиент 1002
        {'customer_id': 1002, 'date': execution_date.strftime('%Y-%m-%d'),
         'session_duration_minutes': 120, 'page_views': 300, 'clicks': 210, 'conversions': 5},
        
        # Клиент 1003
        {'customer_id': 1003, 'date': execution_date.strftime('%Y-%m-%d'),
         'session_duration_minutes': 25, 'page_views': 60, 'clicks': 40, 'conversions': 0},
        {'customer_id': 1003, 'date': (execution_date - timedelta(days=1)).strftime('%Y-%m-%d'),
         'session_duration_minutes': 30, 'page_views': 75, 'clicks': 50, 'conversions': 1},
        
        # Клиент 1004
        {'customer_id': 1004, 'date': execution_date.strftime('%Y-%m-%d'),
         'session_duration_minutes': 90, 'page_views': 200, 'clicks': 150, 'conversions': 3},
        
        # Клиент 1005
        {'customer_id': 1005, 'date': execution_date.strftime('%Y-%m-%d'),
         'session_duration_minutes': 150, 'page_views': 400, 'clicks': 300, 'conversions': 8},
    ]
    
    result = {
        'crm_customers': crm_customers,
        'telemetry_data': telemetry_data,
        'extraction_timestamp': datetime.now().isoformat(),
        'records_count': len(crm_customers) + len(telemetry_data)
    }
    
    # Сохраняем в XCom для передачи между задачами
    task_instance.xcom_push(key='extracted_data', value=result)
    
    logging.info(f"Extracted {len(crm_customers)} customers and {len(telemetry_data)} telemetry records")
    
    return result

def transform_to_data_mart(**context) -> pd.DataFrame:
    """
    Трансформация данных и создание витрины.
    Агрегирует телеметрию по клиентам и объединяет с CRM данными.
    """
    task_instance = context['ti']
    extracted_data = task_instance.xcom_pull(task_ids='extract_from_crm', key='extracted_data')
    
    logging.info("Transforming data and creating data mart...")
    
    # Преобразуем в DataFrame для удобства обработки
    crm_df = pd.DataFrame(extracted_data['crm_customers'])
    telemetry_df = pd.DataFrame(extracted_data['telemetry_data'])
    
    # Агрегация телеметрии по клиентам за последние 30 дней
    # (в реальности здесь был бы фильтр по дате)
    aggregated_telemetry = telemetry_df.groupby('customer_id').agg({
        'session_duration_minutes': ['sum', 'mean', 'count'],
        'page_views': 'sum',
        'clicks': 'sum',
        'conversions': 'sum'
    }).round(2)
    
    # Упрощаем мультииндекс
    aggregated_telemetry.columns = [
        'total_session_minutes',
        'avg_session_minutes',
        'active_days',
        'total_page_views',
        'total_clicks',
        'total_conversions'
    ]
    
    aggregated_telemetry = aggregated_telemetry.reset_index()
    
    # Расчет дополнительных метрик
    aggregated_telemetry['conversion_rate'] = (
        aggregated_telemetry['total_conversions'] / aggregated_telemetry['total_clicks'] * 100
    ).round(2)
    
    aggregated_telemetry['engagement_score'] = (
        aggregated_telemetry['total_session_minutes'] * 0.3 +
        aggregated_telemetry['total_page_views'] * 0.2 +
        aggregated_telemetry['total_clicks'] * 0.5
    ).round(2)
    
    # Объединяем с CRM данными
    data_mart = pd.merge(
        crm_df,
        aggregated_telemetry,
        on='customer_id',
        how='left'
    ).fillna(0)
    
    # Добавляем системные поля
    data_mart['load_date'] = context['execution_date'].strftime('%Y-%m-%d')
    data_mart['created_at'] = datetime.now().isoformat()
    
    # Сохраняем в временный CSV для загрузки
    temp_path = f"/tmp/customer_data_mart_{context['execution_date'].strftime('%Y%m%d')}.csv"
    data_mart.to_csv(temp_path, index=False)
    
    # Сохраняем метаданные в XCom
    metadata = {
        'data_mart_path': temp_path,
        'row_count': len(data_mart),
        'columns': list(data_mart.columns),
        'customer_segments': data_mart['segment'].value_counts().to_dict()
    }
    
    task_instance.xcom_push(key='data_mart_metadata', value=metadata)
    task_instance.xcom_push(key='data_mart_sample', value=data_mart.head(3).to_dict('records'))
    
    logging.info(f"Created data mart with {len(data_mart)} rows. Saved to {temp_path}")
    logging.info(f"Segment distribution: {metadata['customer_segments']}")
    
    return data_mart

def load_to_olap(**context) -> None:
    """
    Загрузка витрины данных в OLAP базу.
    """
    task_instance = context['ti']
    metadata = task_instance.xcom_pull(task_ids='transform_to_data_mart', key='data_mart_metadata')
    
    logging.info(f"Loading data mart to OLAP database from {metadata['data_mart_path']}")
    
    # Подключаемся к OLAP базе
    olap_hook = PostgresHook(postgres_conn_id='olap_postgres')
    
    # SQL для создания/обновления таблицы витрины
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customer_data_mart (
        customer_id INTEGER PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255) UNIQUE,
        company VARCHAR(200),
        segment VARCHAR(50),
        registration_date DATE,
        status VARCHAR(20),
        lifetime_value DECIMAL(12,2),
        
        -- Агрегированные метрики телеметрии
        total_session_minutes DECIMAL(10,2),
        avg_session_minutes DECIMAL(10,2),
        active_days INTEGER,
        total_page_views INTEGER,
        total_clicks INTEGER,
        total_conversions INTEGER,
        conversion_rate DECIMAL(5,2),
        engagement_score DECIMAL(10,2),
        
        -- Системные поля
        load_date DATE,
        created_at TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Индексы для быстрого доступа
    CREATE INDEX IF NOT EXISTS idx_customer_segment ON customer_data_mart(segment);
    CREATE INDEX IF NOT EXISTS idx_customer_email ON customer_data_mart(email);
    CREATE INDEX IF NOT EXISTS idx_customer_company ON customer_data_mart(company);
    CREATE INDEX IF NOT EXISTS idx_customer_engagement ON customer_data_mart(engagement_score DESC);
    CREATE INDEX IF NOT EXISTS idx_load_date ON customer_data_mart(load_date);
    
    -- Комментарии к таблице
    COMMENT ON TABLE customer_data_mart IS 'Витрина данных клиентов с агрегированной телеметрией';
    COMMENT ON COLUMN customer_data_mart.engagement_score IS 'Расчетный показатель вовлеченности (0-100)';
    COMMENT ON COLUMN customer_data_mart.conversion_rate IS 'Процент конверсий от общего числа кликов';
    """
    
    # SQL для загрузки данных (UPSERT - обновление при существовании)
    upsert_sql = """
    INSERT INTO customer_data_mart (
        customer_id, first_name, last_name, email, company, segment,
        registration_date, status, lifetime_value,
        total_session_minutes, avg_session_minutes, active_days,
        total_page_views, total_clicks, total_conversions,
        conversion_rate, engagement_score, load_date, created_at
    )
    SELECT 
        customer_id, first_name, last_name, email, company, segment,
        TO_DATE(registration_date, 'YYYY-MM-DD'), status, lifetime_value,
        total_session_minutes, avg_session_minutes, active_days,
        total_page_views, total_clicks, total_conversions,
        conversion_rate, engagement_score, 
        TO_DATE(load_date, 'YYYY-MM-DD'), 
        TO_TIMESTAMP(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US')
    FROM temp_customer_data
    
    ON CONFLICT (customer_id) 
    DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        email = EXCLUDED.email,
        company = EXCLUDED.company,
        segment = EXCLUDED.segment,
        status = EXCLUDED.status,
        lifetime_value = EXCLUDED.lifetime_value,
        total_session_minutes = EXCLUDED.total_session_minutes,
        avg_session_minutes = EXCLUDED.avg_session_minutes,
        active_days = EXCLUDED.active_days,
        total_page_views = EXCLUDED.total_page_views,
        total_clicks = EXCLUDED.total_clicks,
        total_conversions = EXCLUDED.total_conversions,
        conversion_rate = EXCLUDED.conversion_rate,
        engagement_score = EXCLUDED.engagement_score,
        load_date = EXCLUDED.load_date,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    with olap_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Создаем таблицу если не существует
            cursor.execute(create_table_sql)
            
            # Создаем временную таблицу для загрузки данных
            cursor.execute("DROP TABLE IF EXISTS temp_customer_data;")
            
            # Загружаем данные из CSV через временную таблицу
            cursor.execute(f"""
                CREATE TEMP TABLE temp_customer_data AS 
                SELECT * FROM read_csv('{metadata['data_mart_path']}', auto_detect=true)
            """)
            
            # Выполняем UPSERT
            cursor.execute(upsert_sql)
            
            # Получаем статистику
            cursor.execute("SELECT COUNT(*) FROM customer_data_mart;")
            total_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT segment, COUNT(*), AVG(engagement_score) 
                FROM customer_data_mart 
                GROUP BY segment;
            """)
            segment_stats = cursor.fetchall()
        
        conn.commit()
    
    # Сохраняем статистику загрузки
    load_stats = {
        'total_records': total_count,
        'segment_distribution': {row[0]: {'count': row[1], 'avg_engagement': float(row[2])} 
                                for row in segment_stats},
        'loaded_at': datetime.now().isoformat()
    }
    
    task_instance.xcom_push(key='load_statistics', value=load_stats)
    
    logging.info(f"Successfully loaded {metadata['row_count']} records to OLAP database")
    logging.info(f"Total records in data mart: {total_count}")
    logging.info(f"Segment stats: {segment_stats}")

def validate_data_mart(**context) -> Dict[str, Any]:
    """
    Валидация загруженных данных.
    """
    task_instance = context['ti']
    load_stats = task_instance.xcom_pull(task_ids='load_to_olap', key='load_statistics')
    
    logging.info("Validating data mart...")
    
    olap_hook = PostgresHook(postgres_conn_id='olap_postgres')
    
    validation_queries = {
        'total_records': "SELECT COUNT(*) FROM customer_data_mart;",
        'null_emails': "SELECT COUNT(*) FROM customer_data_mart WHERE email IS NULL;",
        'negative_values': """
            SELECT COUNT(*) FROM customer_data_mart 
            WHERE total_session_minutes < 0 OR engagement_score < 0;
        """,
        'segment_counts': """
            SELECT segment, COUNT(*) 
            FROM customer_data_mart 
            GROUP BY segment 
            ORDER BY COUNT(*) DESC;
        """,
        'data_freshness': """
            SELECT MAX(load_date), MIN(load_date) 
            FROM customer_data_mart;
        """
    }
    
    validation_results = {}
    
    with olap_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for check_name, query in validation_queries.items():
                cursor.execute(query)
                result = cursor.fetchone()
                validation_results[check_name] = result[0] if len(result) == 1 else result
    
    # Проверяем качество данных
    issues = []
    
    if validation_results['null_emails'] > 0:
        issues.append(f"Found {validation_results['null_emails']} records with null emails")
    
    if validation_results['negative_values'] > 0:
        issues.append(f"Found {validation_results['negative_values']} records with negative values")
    
    validation_summary = {
        'validation_timestamp': datetime.now().isoformat(),
        'validation_results': validation_results,
        'issues_found': issues,
        'is_valid': len(issues) == 0,
        'total_records': validation_results['total_records']
    }
    
    task_instance.xcom_push(key='validation_summary', value=validation_summary)
    
    if issues:
        logging.warning(f"Validation issues: {issues}")
    else:
        logging.info("Data mart validation passed successfully")
    
    return validation_summary

def send_success_notification(**context):
    """
    Отправка уведомления об успешном выполнении.
    """
    task_instance = context['ti']
    validation_summary = task_instance.xcom_pull(task_ids='validate_data_mart', key='validation_summary')
    load_stats = task_instance.xcom_pull(task_ids='load_to_olap', key='load_statistics')
    
    execution_date = context['execution_date']
    
    subject = f"[SUCCESS] ETL Pipeline completed for {execution_date.strftime('%Y-%m-%d')}"
    
    html_content = f"""
    <h2>ETL Pipeline Execution Report</h2>
    <p><strong>Execution Date:</strong> {execution_date.strftime('%Y-%m-%d')}</p>
    <p><strong>Status:</strong> ✅ SUCCESS</p>
    
    <h3>Load Statistics</h3>
    <ul>
        <li><strong>Total Records:</strong> {load_stats['total_records']}</li>
    </ul>
    
    <h3>Segment Distribution</h3>
    <ul>
    {"".join([f'<li><strong>{seg}:</strong> {stats["count"]} records (avg engagement: {stats["avg_engagement"]:.2f})</li>' 
              for seg, stats in load_stats['segment_distribution'].items()])}
    </ul>
    
    <h3>Validation Results</h3>
    <ul>
        <li><strong>Total Records Validated:</strong> {validation_summary['total_records']}</li>
        <li><strong>Issues Found:</strong> {len(validation_summary['issues_found'])}</li>
        <li><strong>Data Quality:</strong> {"✅ PASS" if validation_summary['is_valid'] else "⚠️ WARNINGS"}</li>
    </ul>
    
    <p><em>Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
    """
    
    email_task = EmailOperator(
        task_id='send_success_email',
        to=['data-team@bionicpro.com'],
        subject=subject,
        html_content=html_content,
        dag=dag
    )
    
    email_task.execute(context)

# Определение задач DAG
start_task = DummyOperator(
    task_id='start_etl_pipeline',
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_from_crm',
    python_callable=extract_from_crm,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_to_data_mart',
    python_callable=transform_to_data_mart,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_olap',
    python_callable=load_to_olap,
    provide_context=True,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data_mart',
    python_callable=validate_data_mart,
    provide_context=True,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_etl_pipeline',
    dag=dag
)

# Определение порядка выполнения
start_task >> extract_task >> transform_task >> load_task >> validate_task >> end_task

# Условная отправка email только при успешном выполнении
email_on_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag
)

validate_task >> email_on_success