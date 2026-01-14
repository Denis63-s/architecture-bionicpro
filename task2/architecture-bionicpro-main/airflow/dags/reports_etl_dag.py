from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_crm(**context):
    # TODO: здесь будет запрос в CRM (пока заглушка)
    crm_rows = [
        {"user_id": "user1", "client_name": "User One", "prosthesis_model": "BionicX", "country": "RU"},
        {"user_id": "user2", "client_name": "User Two", "prosthesis_model": "BionicX", "country": "RU"},
    ]
    context["ti"].xcom_push(key="crm_rows", value=crm_rows)


def extract_telemetry(**context):
    # TODO: здесь будет запрос в DB телеметрии (пока заглушка)
    telemetry_rows = [
        {"user_id": "user1", "steps": 1200, "load": 0.3, "alerts": 1},
        {"user_id": "user1", "steps": 800, "load": 0.4, "alerts": 0},
        {"user_id": "user2", "steps": 400, "load": 0.2, "alerts": 2},
    ]
    context["ti"].xcom_push(key="telemetry_rows", value=telemetry_rows)


def transform_join(**context):
    crm_rows = context["ti"].xcom_pull(key="crm_rows")
    telemetry_rows = context["ti"].xcom_pull(key="telemetry_rows")

    # агрегируем телеметрию по user_id
    agg = {}
    for r in telemetry_rows:
        uid = r["user_id"]
        agg.setdefault(uid, {"cnt": 0, "sum_steps": 0, "sum_load": 0, "max_load": 0, "sum_alerts": 0})
        agg[uid]["cnt"] += 1
        agg[uid]["sum_steps"] += r["steps"]
        agg[uid]["sum_load"] += r["load"]
        agg[uid]["max_load"] = max(agg[uid]["max_load"], r["load"])
        agg[uid]["sum_alerts"] += r["alerts"]

    # join с CRM
    mart_rows = []
    for c in crm_rows:
        uid = c["user_id"]
        a = agg.get(uid, {"cnt": 0, "sum_steps": 0, "sum_load": 0, "max_load": 0, "sum_alerts": 0})
        cnt = max(a["cnt"], 1)
        mart_rows.append({
            "user_id": uid,
            "period_start": (datetime.utcnow() - timedelta(days=1)).date().isoformat(),
            "period_end": datetime.utcnow().date().isoformat(),
            "avg_steps": a["sum_steps"] / cnt,
            "avg_load": a["sum_load"] / cnt,
            "max_load": a["max_load"],
            "alerts_count": a["sum_alerts"],
            "client_name": c["client_name"],
            "prosthesis_model": c["prosthesis_model"],
            "country": c["country"],
        })

    context["ti"].xcom_push(key="mart_rows", value=mart_rows)


def load_to_olap(**context):
    mart_rows = context["ti"].xcom_pull(key="mart_rows")
    # TODO: заменить на реальную загрузку в OLAP (ClickHouse/Postgres)
    # Пока: просто выводим (Airflow logs)
    print("Loading to OLAP mart reports_mart:")
    for r in mart_rows:
        print(r)


default_args = {
    "owner": "bionicpro",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="reports_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",  # каждый день в 02:00
    catchup=False,
    tags=["reports", "etl"],
) as dag:

    t1 = PythonOperator(task_id="extract_crm", python_callable=extract_crm)
    t2 = PythonOperator(task_id="extract_telemetry", python_callable=extract_telemetry)
    t3 = PythonOperator(task_id="transform_join", python_callable=transform_join)
    t4 = PythonOperator(task_id="load_to_olap", python_callable=load_to_olap)

    [t1, t2] >> t3 >> t4
