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

    # Запит 1: В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду вересня?
    tv_brand_analysis = RedshiftDataOperator(
        task_id='tv_brand_analysis_20_30_age',
        workgroup_name=WORKGROUP_NAME,
        database=DATABASE_NAME,
        sql="""
            DROP TABLE IF EXISTS gold.tv_analysis_result;
            CREATE TABLE gold.tv_analysis_result AS
            WITH base AS (
                SELECT
                    s.client_id,
                    yc.state,
                    LOWER(TRIM(s.product_name)) AS product_name_norm,
                    CAST(s.purchase_date AS DATE) AS purchase_date,
                    CAST(yc.birth_date AS DATE) AS birth_date
                FROM silver_ext.sales s
                JOIN gold.user_profiles_enriched yc
                    ON s.client_id = yc.client_id
                WHERE yc.birth_date IS NOT NULL
                AND yc.state IS NOT NULL
                AND yc.state <> ''
            ),
            state_sales AS (
                SELECT
                    state,
                    COUNT(*) AS sales_count
                FROM base
                WHERE product_name_norm = 'tv'
                AND purchase_date BETWEEN DATE '2022-09-01' AND DATE '2022-09-10'
                AND DATEDIFF(year, birth_date, purchase_date) BETWEEN 20 AND 30
                GROUP BY state
            )
            SELECT *
            FROM state_sales
            WHERE sales_count = (SELECT MAX(sales_count) FROM state_sales);
        """,
        wait_for_completion=True
    )
    
    tv_brand_analysis
