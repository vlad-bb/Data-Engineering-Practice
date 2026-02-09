from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

WORKGROUP_NAME = "data-platform-workgroup"
DATABASE_NAME = "dev"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'run_analytical_queries',
    default_args=default_args,
    description='Business Intelligence queries on top of Gold and Silver layers',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['bi', 'analysis', 'redshift'],
) as dag:

    # Запит 1: Топ-3 штати за продажами ТБ серед молоді
    tv_brand_analysis = RedshiftDataOperator(
        task_id='tv_brand_analysis_20_30_age',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE_NAME,
        sql="""
            DROP TABLE IF EXISTS gold.tv_analysis_result;
            CREATE TABLE gold.tv_analysis_result AS
            WITH young_customers AS (
                SELECT client_id, state, 
                       DATEDIFF(year, birth_date, GETDATE()) as age
                FROM gold.user_profiles_enriched
                WHERE birth_date IS NOT NULL 
                AND state IS NOT NULL AND state != ''
            )
            SELECT s.product_name, yc.state, COUNT(*) as sales_count
            FROM silver_ext.sales s
            JOIN young_customers yc ON s.client_id = yc.client_id
            WHERE yc.age BETWEEN 20 AND 30
            AND s.product_name = 'TV'
            GROUP BY 1, 2
            ORDER BY sales_count DESC
            LIMIT 3;
        """,
        wait_for_completion=True
    )
    
    tv_brand_analysis
