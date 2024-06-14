from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'spark_oltp_to_olap',
    default_args=default_args,
    description='DAG for processing OLTP to OLAP and storing result in OLAP database',
    schedule_interval='@daily',
)

# Task to run Spark job
spark_job_project = SparkSubmitOperator(
    task_id='spark_oltp_to_olap',
    application='/home/fauzipersib/airflow/dags/spark_job_project.py',
    name='spark_oltp_to_olap',
    conn_id='fauzipostgresconn',
    verbose=1,
    dag=dag,
)

# Task to create OLAP table in PostgreSQL
create_olap_table = PostgresOperator(
    task_id='create_olap_table',
    postgres_conn_id='fauzipostgresconn',
    sql='''
        CREATE TABLE IF NOT EXISTS sql_data (
            no.mc int8,
            nama_customer VARCHAR(27),
            nama_produk VARCHAR(100),
            order VARCHAR(11),
            status VARCHAR(11)
        );
    ''',
    dag=dag,
)

# Task to load data into the OLAP table
load_data_to_olap = PostgresOperator(
    task_id='load_data_to_olap',
    postgres_conn_id='fauzipostgresconn',
    sql='''
        INSERT INTO sql_data (no.mc, nama_customer, nama_produk, order, status)
        SELECT no.mc, nama_customer, nama_produk, order, status
        FROM staging_sql_data;
    ''',
    dag=dag,
)

# Setting task dependencies
spark_job_project >> create_olap_table >> load_data_to_olap