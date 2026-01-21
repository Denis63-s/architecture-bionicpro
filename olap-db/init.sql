-- Инициализация OLAP базы данных для витрины

-- Создание расширения для улучшенной работы с JSON (если нужно)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Таблица витрины данных клиентов
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

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_customer_segment ON customer_data_mart(segment);
CREATE INDEX IF NOT EXISTS idx_customer_email ON customer_data_mart(email);
CREATE INDEX IF NOT EXISTS idx_customer_company ON customer_data_mart(company);
CREATE INDEX IF NOT EXISTS idx_customer_engagement ON customer_data_mart(engagement_score DESC);
CREATE INDEX IF NOT EXISTS idx_load_date ON customer_data_mart(load_date);
CREATE INDEX IF NOT EXISTS idx_customer_status ON customer_data_mart(status);
CREATE INDEX IF NOT EXISTS idx_registration_date ON customer_data_mart(registration_date);

-- Партиционирование по дате загрузки (для больших объемов данных)
-- CREATE TABLE customer_data_mart_y2023 PARTITION OF customer_data_mart
-- FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

-- Таблица для аудита ETL процессов
CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100),
    execution_date DATE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_processed INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс для быстрого поиска по аудит логам
CREATE INDEX IF NOT EXISTS idx_audit_process_date ON etl_audit_log(process_name, execution_date);
CREATE INDEX IF NOT EXISTS idx_audit_status ON etl_audit_log(status);

-- Таблица справочник сегментов
CREATE TABLE IF NOT EXISTS customer_segments (
    segment_code VARCHAR(50) PRIMARY KEY,
    segment_name VARCHAR(100),
    description TEXT,
    min_engagement_score DECIMAL(10,2),
    max_engagement_score DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Наполнение справочника сегментов
INSERT INTO customer_segments (segment_code, segment_name, description, min_engagement_score, max_engagement_score) VALUES
('standard', 'Стандартный', 'Базовые клиенты с умеренной активностью', 0, 50),
('premium', 'Премиум', 'Активные клиенты с высоким вовлечением', 50, 80),
('enterprise', 'Корпоративный', 'Ключевые клиенты с максимальной активностью', 80, 100),
('inactive', 'Неактивный', 'Клиенты без активности в последний период', 0, 10)
ON CONFLICT (segment_code) DO NOTHING;

-- Представление для удобства доступа к данным
CREATE OR REPLACE VIEW v_customer_analytics AS
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) as full_name,
    c.email,
    c.company,
    c.segment,
    s.segment_name,
    c.registration_date,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.registration_date)) as years_as_customer,
    c.status,
    c.lifetime_value,
    c.total_session_minutes,
    c.avg_session_minutes,
    c.active_days,
    c.total_page_views,
    c.total_clicks,
    c.total_conversions,
    c.conversion_rate,
    c.engagement_score,
    CASE 
        WHEN c.engagement_score >= s.min_engagement_score AND c.engagement_score <= s.max_engagement_score 
        THEN 'В рамках сегмента'
        ELSE 'Требует пересмотра сегмента'
    END as segment_validation,
    c.load_date,
    c.updated_at
FROM customer_data_mart c
LEFT JOIN customer_segments s ON c.segment = s.segment_code;

-- Материализованное представление для агрегированной статистики
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_segment_performance AS
SELECT 
    segment,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_lifetime_value,
    AVG(engagement_score) as avg_engagement,
    SUM(total_conversions) as total_conversions,
    AVG(conversion_rate) as avg_conversion_rate,
    MAX(load_date) as last_update_date
FROM customer_data_mart
GROUP BY segment
WITH DATA;

-- Индекс для материализованного представления
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_segment ON mv_segment_performance(segment);

-- Функция для обновления материализованного представления
CREATE OR REPLACE FUNCTION refresh_segment_performance()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_segment_performance;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автоматического обновления представления
DROP TRIGGER IF EXISTS trg_refresh_mv ON customer_data_mart;
CREATE TRIGGER trg_refresh_mv
AFTER INSERT OR UPDATE OR DELETE ON customer_data_mart
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_segment_performance();

-- Комментарии к объектам БД
COMMENT ON TABLE customer_data_mart IS 'Основная витрина данных клиентов с агрегированной телеметрией';
COMMENT ON VIEW v_customer_analytics IS 'Представление для аналитики клиентов с дополнительными расчетными полями';
COMMENT ON MATERIALIZED VIEW mv_segment_performance IS 'Материализованное представление с агрегированной статистикой по сегментам';

-- Права доступа (если используются разные пользователи)
GRANT SELECT ON customer_data_mart TO olap_user;
GRANT SELECT ON v_customer_analytics TO olap_user;
GRANT SELECT ON mv_segment_performance TO olap_user;
GRANT SELECT ON etl_audit_log TO olap_user;
GRANT SELECT ON customer_segments TO olap_user;

-- Тестовые данные для демонстрации
INSERT INTO customer_data_mart (
    customer_id, first_name, last_name, email, company, segment,
    registration_date, status, lifetime_value,
    total_session_minutes, avg_session_minutes, active_days,
    total_page_views, total_clicks, total_conversions,
    conversion_rate, engagement_score, load_date, created_at
) VALUES
(1001, 'Иван', 'Иванов', 'ivan.ivanov@example.com', 'TechCorp', 'premium',
 '2023-01-15', 'active', 15000.50,
 1250.75, 45.3, 28, 3500, 2450, 45, 1.84, 72.5, '2024-01-15', NOW()),
(1002, 'Мария', 'Петрова', 'maria.petrova@example.com', 'BusinessSoft', 'enterprise',
 '2023-03-20', 'active', 25000.75,
 3150.25, 105.2, 30, 8900, 6200, 125, 2.02, 88.3, '2024-01-15', NOW()),
(1003, 'Алексей', 'Сидоров', 'alexey.sidorov@example.com', 'StartupInc', 'standard',
 '2023-06-10', 'active', 8000.25,
 650.50, 32.5, 20, 1800, 1250, 18, 1.44, 42.8, '2024-01-15', NOW())
ON CONFLICT (customer_id) DO NOTHING;