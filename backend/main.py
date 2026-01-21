from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, List, Dict
import asyncpg
import os
from datetime import datetime
from jose import jwt, JWTError

# Конфигурация
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://olap_user:olap_password@olap-db:5432/olap_db")
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080/auth/realms/reports-realm")

app = FastAPI(
    title="Reports API",
    version="1.0",
    description="API для получения отчетов из OLAP витрины с ограничением доступа"
)
security = HTTPBearer()

# Кэш для хранения соответствий email -> customer_id
user_customer_cache: Dict[str, int] = {}

# Подключение к БД
async def get_db():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()

# Проверка токена
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.get_unverified_claims(token)
        email = payload.get("email") or payload.get("preferred_username")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token: no email")
        return {"email": email, "payload": payload}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Получить customer_id для пользователя
async def get_customer_id_for_user(email: str, conn) -> Optional[int]:
    """Получить customer_id для пользователя по email"""
    if email in user_customer_cache:
        return user_customer_cache[email]
    
    query = "SELECT customer_id FROM customer_data_mart WHERE email = $1 LIMIT 1"
    customer_id = await conn.fetchval(query, email)
    
    if customer_id:
        user_customer_cache[email] = customer_id
    
    return customer_id

# СТРОГАЯ ПРОВЕРКА ДОСТУПА (Task 4)
async def strict_access_check(
    user_info: dict, 
    requested_customer_id: Optional[int], 
    conn
) -> int:
    """
    Строгая проверка доступа:
    1. Получаем customer_id пользователя по email
    2. Если запрашивается конкретный customer_id - проверяем совпадение
    3. Возвращаем разрешенный customer_id
    """
    user_email = user_info["email"]
    user_customer_id = await get_customer_id_for_user(user_email, conn)
    
    if not user_customer_id:
        raise HTTPException(
            status_code=403, 
            detail="No customer profile found. Please contact administrator."
        )
    
    # Если запрашивается конкретный customer_id
    if requested_customer_id:
        if requested_customer_id != user_customer_id:
            raise HTTPException(
                status_code=403, 
                detail=f"Access denied. You can only access your own data (customer_id: {user_customer_id})"
            )
        return requested_customer_id
    
    # Если customer_id не указан - возвращаем customer_id пользователя
    return user_customer_id

@app.get("/")
async def root():
    return {
        "message": "Reports API", 
        "status": "running",
        "version": "1.0",
        "endpoints": {
            "reports": "/reports",
            "generate_report": "/reports/generate",
            "report_status": "/reports/status/{customer_id}",
            "health": "/health"
        }
    }

@app.get("/health")
async def health(conn = Depends(get_db)):
    """Проверка здоровья сервиса и БД"""
    db_status = False
    try:
        result = await conn.fetchval("SELECT 1")
        db_status = result == 1
    except:
        db_status = False
    
    return {
        "status": "healthy" if db_status else "unhealthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": db_status,
        "service": "reports-api"
    }

# ЭНДПОИНТ ДЛЯ ПОЛУЧЕНИЯ ОТЧЕТОВ (Task 3)
@app.get("/reports")
async def get_reports(
    customer_id: Optional[int] = Query(None, description="ID клиента (опционально)"),
    user_info: dict = Depends(verify_token),
    conn = Depends(get_db)
):
    """
    Получить отчеты ТОЛЬКО для себя.
    
    Безопасность:
    - Если customer_id указан: проверяем, что он принадлежит пользователю
    - Если не указан: возвращаем данные пользователя
    - Всегда фильтруем по email пользователя из токена
    """
    
    # Строгая проверка доступа
    allowed_customer_id = await strict_access_check(user_info, customer_id, conn)
    user_email = user_info["email"]
    
    # Запрос данных только для разрешенного customer_id
    query = """
        SELECT 
            customer_id,
            COALESCE(CONCAT(first_name, ' ', last_name), 'User') as name,
            email,
            segment,
            registration_date,
            status,
            lifetime_value,
            total_session_minutes,
            avg_session_minutes,
            active_days,
            total_page_views,
            total_clicks,
            total_conversions,
            conversion_rate,
            engagement_score,
            load_date,
            updated_at,
            created_at
        FROM customer_data_mart 
        WHERE customer_id = $1
        ORDER BY updated_at DESC
    """
    
    rows = await conn.fetch(query, allowed_customer_id)
    
    if not rows:
        raise HTTPException(
            status_code=404, 
            detail=f"No reports found for customer_id: {allowed_customer_id}"
        )
    
    # Преобразование результатов
    reports = []
    for row in rows:
        # Расчет дополнительных метрик
        total_hours = float(row["total_session_minutes"]) / 60 if row["total_session_minutes"] else 0
        avg_daily_hours = total_hours / row["active_days"] if row["active_days"] > 0 else 0
        actions_per_hour = row["total_clicks"] / total_hours if total_hours > 0 else 0
        
        reports.append({
            "customer": {
                "id": row["customer_id"],
                "name": row["name"],
                "email": row["email"],
                "segment": row["segment"],
                "registration_date": row["registration_date"].isoformat() if row["registration_date"] else None,
                "status": row["status"]
            },
            "telemetry": {
                "total_hours": round(total_hours, 2),
                "avg_daily_hours": round(avg_daily_hours, 2),
                "active_days": row["active_days"],
                "total_page_views": row["total_page_views"],
                "total_clicks": row["total_clicks"],
                "total_conversions": row["total_conversions"],
                "conversion_rate": round(float(row["conversion_rate"] or 0), 2),
                "engagement_score": round(float(row["engagement_score"] or 0), 2),
                "actions_per_hour": round(actions_per_hour, 2)
            },
            "financial": {
                "lifetime_value": round(float(row["lifetime_value"] or 0), 2),
                "value_per_hour": round(float(row["lifetime_value"] or 0) / total_hours, 2) if total_hours > 0 else 0
            },
            "metadata": {
                "load_date": row["load_date"].isoformat() if row["load_date"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "data_freshness": "fresh" if row["updated_at"] and (datetime.utcnow() - row["updated_at"]).days < 1 else "stale"
            }
        })
    
    return {
        "user": {
            "email": user_email,
            "customer_id": allowed_customer_id,
            "access_level": "self_only"
        },
        "reports": reports,
        "count": len(reports),
        "message": "Access restricted to your own data only"
    }

# ЭНДПОИНТ ДЛЯ ГЕНЕРАЦИИ ОТЧЕТА (Task 5 - для кнопки)
@app.post("/reports/generate")
async def generate_report(
    user_info: dict = Depends(verify_token),
    conn = Depends(get_db)
):
    """
    Запустить генерацию нового отчета для пользователя.
    Этот эндпоинт вызывается кнопкой в UI.
    
    В реальности здесь бы:
    1. Отправлялся запрос в Airflow для запуска DAG
    2. Или обновлялись данные в витрине
    
    Для демо: просто обновляем timestamp и возвращаем сообщение
    """
    
    user_email = user_info["email"]
    user_customer_id = await get_customer_id_for_user(user_email, conn)
    
    if not user_customer_id:
        raise HTTPException(
            status_code=404, 
            detail="User profile not found in data mart"
        )
    
    # Имитация запуска генерации отчета
    # В реальности здесь был бы вызов Airflow API или Celery task
    
    update_query = """
        UPDATE customer_data_mart 
        SET updated_at = NOW(),
            load_date = CURRENT_DATE
        WHERE customer_id = $1
        RETURNING updated_at, customer_id, email
    """
    
    try:
        result = await conn.fetchrow(update_query, user_customer_id)
        
        # Логирование действия
        log_query = """
            INSERT INTO etl_audit_log 
            (process_name, execution_date, start_time, end_time, records_processed, status)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        await conn.execute(
            log_query,
            "manual_report_generation",
            datetime.utcnow().date(),
            datetime.utcnow(),
            datetime.utcnow(),
            1,  # одна запись обновлена
            "success"
        )
        
        return {
            "status": "success",
            "message": "Report generation initiated successfully",
            "details": {
                "customer_id": user_customer_id,
                "user_email": user_email,
                "generated_at": result["updated_at"].isoformat() if result["updated_at"] else None,
                "estimated_completion": "Data will be refreshed within 2 minutes",
                "next_steps": "Use /reports endpoint to get updated data"
            },
            "ui_action": {
                "show_notification": True,
                "notification_message": "Report generation started! Data will update shortly.",
                "refresh_after_seconds": 120
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to generate report: {str(e)}"
        )

# ЭНДПОИНТ ДЛЯ ПРОВЕРКИ СТАТУСА (Task 5 - для кнопки)
@app.get("/reports/status")
async def get_report_status(
    user_info: dict = Depends(verify_token),
    conn = Depends(get_db)
):
    """
    Проверить статус данных пользователя.
    Используется UI для отображения свежести данных.
    """
    
    user_email = user_info["email"]
    user_customer_id = await get_customer_id_for_user(user_email, conn)
    
    if not user_customer_id:
        raise HTTPException(status_code=404, detail="User profile not found")
    
    query = """
        SELECT 
            updated_at,
            load_date,
            COUNT(*) as record_count,
            CASE 
                WHEN updated_at > NOW() - INTERVAL '5 minutes' THEN 'fresh'
                WHEN updated_at > NOW() - INTERVAL '1 hour' THEN 'recent' 
                WHEN updated_at > NOW() - INTERVAL '24 hours' THEN 'daily'
                ELSE 'stale'
            END as freshness,
            CASE 
                WHEN updated_at > NOW() - INTERVAL '1 hour' THEN 'up_to_date'
                ELSE 'needs_update'
            END as recommendation
        FROM customer_data_mart 
        WHERE customer_id = $1
        GROUP BY updated_at, load_date
    """
    
    row = await conn.fetchrow(query, user_customer_id)
    
    if not row:
        raise HTTPException(status_code=404, detail="No report data found")
    
    # Определяем цвет для UI
    freshness_colors = {
        "fresh": "green",
        "recent": "blue", 
        "daily": "yellow",
        "stale": "red"
    }
    
    return {
        "status": {
            "customer_id": user_customer_id,
            "last_updated": row["updated_at"].isoformat() if row["updated_at"] else None,
            "load_date": row["load_date"].isoformat() if row["load_date"] else None,
            "freshness": row["freshness"],
            "freshness_color": freshness_colors.get(row["freshness"], "gray"),
            "record_count": row["record_count"],
            "recommendation": row["recommendation"],
            "is_action_required": row["recommendation"] == "needs_update"
        },
        "ui": {
            "show_warning": row["recommendation"] == "needs_update",
            "warning_message": f"Data is {row['freshness']}. Consider generating a new report.",
            "button_state": "enabled",
            "last_check": datetime.utcnow().isoformat()
        }
    }

# ЭНДПОИНТ ДЛЯ ПОЛУЧЕНИЯ СЕГМЕНТОВ (опционально)
@app.get("/reports/segments")
async def get_segments(
    user_info: dict = Depends(verify_token),
    conn = Depends(get_db)
):
    """Получить список сегментов пользователя"""
    user_email = user_info["email"]
    
    query = """
        SELECT DISTINCT segment 
        FROM customer_data_mart 
        WHERE email = $1 AND segment IS NOT NULL
        ORDER BY segment
    """
    
    rows = await conn.fetch(query, user_email)
    
    return {
        "segments": [row["segment"] for row in rows],
        "count": len(rows)
    }

# ЭНДПОИНТ ДЛЯ СВОДКИ (опционально)
@app.get("/reports/summary")
async def get_summary(
    user_info: dict = Depends(verify_token),
    conn = Depends(get_db)
):
    """Сводка по данным пользователя"""
    user_email = user_info["email"]
    user_customer_id = await get_customer_id_for_user(user_email, conn)
    
    if not user_customer_id:
        raise HTTPException(status_code=404, detail="User data not found")
    
    query = """
        SELECT 
            COUNT(*) as total_records,
            AVG(engagement_score) as avg_engagement,
            SUM(total_conversions) as total_conversions,
            AVG(lifetime_value) as avg_lifetime_value,
            SUM(total_session_minutes) as total_minutes,
            MAX(updated_at) as last_update,
            MIN(load_date) as first_load
        FROM customer_data_mart 
        WHERE customer_id = $1
    """
    
    row = await conn.fetchrow(query, user_customer_id)
    
    if not row:
        raise HTTPException(status_code=404, detail="No summary data found")
    
    return {
        "summary": {
            "customer_id": user_customer_id,
            "total_records": row["total_records"],
            "avg_engagement": round(float(row["avg_engagement"] or 0), 2),
            "total_conversions": row["total_conversions"] or 0,
            "avg_lifetime_value": round(float(row["avg_lifetime_value"] or 0), 2),
            "total_usage_hours": round(float(row["total_minutes"] or 0) / 60, 1),
            "last_update": row["last_update"].isoformat() if row["last_update"] else None,
            "first_load": row["first_load"].isoformat() if row["first_load"] else None,
            "reporting_period_days": (datetime.utcnow().date() - row["first_load"]).days if row["first_load"] else 0
        },
        "recommendations": {
            "generate_new": row["last_update"] and (datetime.utcnow() - row["last_update"]).days > 1,
            "message": "Data is up to date" if row["last_update"] and (datetime.utcnow() - row["last_update"]).days <= 1 else "Consider generating new report"
        }
    }

# Тестовый эндпоинт для проверки (только для разработки)
@app.get("/debug/user-info")
async def debug_user_info(user_info: dict = Depends(verify_token)):
    """Отладочная информация о пользователе (только для разработки)"""
    return {
        "user_email": user_info["email"],
        "token_info": {
            "has_email": "email" in user_info["payload"],
            "has_username": "preferred_username" in user_info["payload"],
            "audience": user_info["payload"].get("aud"),
            "issued_at": user_info["payload"].get("iat"),
            "expires_at": user_info["payload"].get("exp")
        },
        "note": "This endpoint is for development only"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)