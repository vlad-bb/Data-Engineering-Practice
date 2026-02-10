from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import time
import logging
import os

# Параметри
WORKGROUP_NAME = "data-platform-workgroup"
DATABASE_NAME = "dev"
ROLE_ARN = os.environ.get("REDSHIFT_ROLE_ARN")
REGION = os.environ.get("AWS_REGION")

def execute_redshift_sql(sql_query, task_id):
    """
    Кастомна функція для виконання SQL через Redshift Data API
    з детальним виводом помилок у логи Airflow.
    """
    client = boto3.client('redshift-data', region_name=REGION)
    
    logging.info(f"Executing SQL for task {task_id}: {sql_query}")
    
    # Запускаємо запит
    response = client.execute_statement(
        WorkgroupName=WORKGROUP_NAME,
        Database=DATABASE_NAME,
        Sql=sql_query,
        StatementName=f"Airflow_{task_id}_{int(time.time())}"
    )
    
    statement_id = response['Id']
    logging.info(f"Statement ID: {statement_id}")
    
    # Чекаємо на результат
    while True:
        status_resp = client.describe_statement(Id=statement_id)
        status = status_resp['Status']
        
        if status == 'FINISHED':
            logging.info("Query finished successfully.")
            return True
        elif status == 'FAILED':
            error_msg = status_resp.get('Error', 'Unknown Redshift Error')
            # ЦЕ ТЕ, ЩО НАМ ПОТРІБНО: Виводимо помилку в лог
            logging.error("=" * 50)
            logging.error(f"REDSHIFT QUERY FAILED!")
            logging.error(f"TASK: {task_id}")
            logging.error(f"ERROR DETAILS: {error_msg}")
            logging.error(f"SQL: {sql_query}")
            logging.error("=" * 50)
            raise Exception(f"Redshift Query Failed: {error_msg}")
        elif status in ['ABORTED']:
            raise Exception(f"Query was aborted.")
            
        time.sleep(2)

default_args = {
    'owner': 'airflow',
    'retries': 0, # Вимикаємо ретраї, щоб швидше бачити помилку
}

with DAG(
    'gold_layer_enrichment',
    default_args=default_args,
    description='Debuggable Gold Enrichment DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['debug', 'gold'],
) as dag:

    # Крок 1: Зовнішня схема
    task_setup_spectrum = PythonOperator(
        task_id='setup_external_schema',
        python_callable=execute_redshift_sql,
        op_kwargs={
            'task_id': 'setup_external_schema',
            'sql_query': f"""
                DROP SCHEMA IF EXISTS silver_ext CASCADE;
                CREATE EXTERNAL SCHEMA silver_ext
                FROM DATA CATALOG DATABASE 'silver'
                IAM_ROLE '{ROLE_ARN}'
                REGION '{REGION}';
            """
        }
    )

    # Крок 2: Таблиці Gold
    task_setup_gold = PythonOperator(
        task_id='setup_gold_layers',
        python_callable=execute_redshift_sql,
        op_kwargs={
            'task_id': 'setup_gold_layers',
            'sql_query': """
                CREATE SCHEMA IF NOT EXISTS gold;
                CREATE TABLE IF NOT EXISTS gold.user_profiles_enriched (
                    client_id INT,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    registration_date DATE,
                    state VARCHAR(50),
                    birth_date DATE,
                    phone_number VARCHAR(50)
                );
            """
        }
    )

    # Крок 3: MERGE
    task_merge = PythonOperator(
        task_id='run_merge_enrichment',
        python_callable=execute_redshift_sql,
        op_kwargs={
            'task_id': 'run_merge_enrichment',
            'sql_query': """
                MERGE INTO gold.user_profiles_enriched
                USING (
                    SELECT 
                        c.client_id, 
                        COALESCE(NULLIF(TRIM(c.first_name), ''), NULLIF(TRIM(SPLIT_PART(u.full_name, ' ', 1)), '')) as first_name,
                        COALESCE(NULLIF(TRIM(c.last_name), ''), NULLIF(TRIM(SPLIT_PART(u.full_name, ' ', 2)), '')) as last_name,
                        TRIM(c.email) as email,
                        CAST(c.registration_date AS DATE) as registration_date,
                        COALESCE(NULLIF(TRIM(c.state), ''), NULLIF(TRIM(u.state), '')) as state,
                        CAST(u.birth_date AS DATE) as birth_date, 
                        NULLIF(TRIM(u.phone_number), '') as phone_number
                    FROM silver_ext.customers c
                    LEFT JOIN silver_ext.user_profiles u ON TRIM(LOWER(c.email)) = TRIM(LOWER(u.email))
                    WHERE c.client_id IS NOT NULL
                ) AS source
                ON gold.user_profiles_enriched.client_id = source.client_id
                WHEN MATCHED THEN UPDATE SET
                    first_name = source.first_name,
                    last_name = source.last_name,
                    email = source.email,
                    state = source.state, 
                    birth_date = source.birth_date, 
                    phone_number = source.phone_number
                WHEN NOT MATCHED THEN INSERT (client_id, first_name, last_name, email, registration_date, state, birth_date, phone_number)
                VALUES (source.client_id, source.first_name, source.last_name, source.email, source.registration_date, source.state, source.birth_date, source.phone_number);
            """
        }
    )

    task_setup_spectrum >> task_setup_gold >> task_merge
