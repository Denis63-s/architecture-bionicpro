**Замена Code Grant на PKCE в Keycloak**
- Обновлен realm-export.json для поддержки PKCE
- Настроен фронтенд для использования PKCE flow
- Отключен небезопасный directAccessGrantsEnabled

**ETL процесс с Airflow**
- Создан Airflow DAG crm_to_olap_etl_dag.py
- Реализован ETL процесс из CRM в OLAP базу
- Создана витрина данных customer_data_mart
- Настроено расписание (ежедневно в 02:00)

**Бэкенд API на Python/FastAPI**
- Создан сервис backend/ с FastAPI
- Реализован эндпоинт /reports для получения данных из OLAP
- Все вычисления выполняются на уровне БД (OLAP витрина)

**Ограничение доступа к эндпоинтам**
- Реализована строгая проверка JWT токенов от Keycloak
- Пользователь может получать данные ТОЛЬКО по своему customer_id
- Email из токена сопоставляется с данными в витрине



**Быстрый запуск**
# 1. Клонировать/распаковать проект
cd architecture-bionicpro

# 2. Запустить все сервисы
docker-compose up -d

# 3. Дождаться запуска (2-3 минуты)

**Доступ к сервисам**
Frontend: http://localhost:3000
Backend API: http://localhost:8000/docs
Keycloak Admin: http://localhost:8080 (admin/admin)
Airflow: http://localhost:8081 (admin/admin)
OLAP DB: localhost:5434 (olap_user/olap_password)

**Тестовые пользователи**
Email: ivan.ivanov@example.com / Password: password123
Email: maria.petrova@example.com / Password: password123  
Email: admin1@example.com / Password: admin123

**Проверка работы**
Откройте http://localhost:3000
Войдите под тестовым пользователем
Нажмите "Generate New Report"
Через 5-10 секунд данные обновятся
Проверьте отчеты