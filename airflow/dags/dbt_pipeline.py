from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests


default_args = {
    "owner": "luis",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_trino_iceberg_pipeline",
    default_args=default_args,
    description="Pipeline dbt com Trino + Iceberg",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "iceberg", "trino"],
) as dag:

    # 👇 NOVO STEP - chama n8n
    def call_n8n():
        url = "http://n8n:5678/webhook/2afc0f63-bfb4-45ff-83c9-6b4389b1e6c0"
        
        response = requests.post(url, timeout=30)
        
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")

        response.raise_for_status()  # faz a task falhar se der erro

    trigger_n8n = PythonOperator(
        task_id="trigger_n8n_webhook",
        python_callable=call_n8n,
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="""
        echo "🔍 Rodando dbt debug..."
        docker exec dbt dbt debug
        """,
        execution_timeout=timedelta(minutes=5),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        echo "🚀 Rodando dbt run..."
        docker exec dbt dbt run
        """,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        echo "🧪 Rodando dbt test..."
        docker exec dbt dbt test
        """,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="""
        echo "📚 Gerando documentação..."
        docker exec dbt dbt docs generate
        """,
        execution_timeout=timedelta(minutes=5),
    )

    # 👇 ORDEM COMPLETA
    trigger_n8n >> dbt_debug >> dbt_run >> dbt_test >> dbt_docs