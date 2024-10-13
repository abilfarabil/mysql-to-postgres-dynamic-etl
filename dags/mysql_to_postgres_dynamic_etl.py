from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import mysql.connector
import psycopg2
import logging

# Fungsi untuk setiap task yang akan menjalankan proses ETL
def etl_task(table_name):
    try:
        # Koneksi ke MySQL
        mysql_conn = mysql.connector.connect(
            host='mysql',  # Nama service MySQL di docker-compose
            user='etl_user',
            password='etl_password',
            database='etl_database'
        )
        cursor = mysql_conn.cursor()

        # Ekstraksi data dari MySQL
        cursor.execute(f"SELECT * FROM {table_name};")
        records = cursor.fetchall()

        # Koneksi ke PostgreSQL
        postgres_conn = psycopg2.connect(
            host='postgres',  # Nama service PostgreSQL di docker-compose
            user='etl_user',
            password='etl_password',
            dbname='etl_database'
        )
        pg_cursor = postgres_conn.cursor()

        # Set search_path ke schema yang benar
        pg_cursor.execute('SET search_path TO etl, public;')

        # Memuat data ke PostgreSQL
        for record in records:
            placeholders = ', '.join(['%s'] * len(record))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            pg_cursor.execute(sql, record)

        # Commit dan tutup koneksi
        postgres_conn.commit()
        pg_cursor.close()
        postgres_conn.close()
        cursor.close()
        mysql_conn.close()

        logging.info(f"Data from {table_name} has been loaded into PostgreSQL.")

    except Exception as e:
        logging.error(f"Error loading data from {table_name}: {str(e)}")
        raise  # Raise the exception to let Airflow know the task has failed

# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Mengatur tanggal mulai
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definisikan DAG
with DAG(
    'mysql_to_postgres_dynamic_etl',
    default_args=default_args,
    description='Dynamic ETL from MySQL to PostgreSQL',
    schedule_interval='15 9-21/2 * * 5#1,5',  # Menjalankan setiap 2 jam di jam 9:15 - 21:15 pada minggu pertama dan ketiga setiap Jumat
    catchup=False
) as dag:

    # Dummy task untuk memulai
    start_task = DummyOperator(task_id='start')

    # Daftar tabel yang akan diekstraksi
    tables = ['categories', 'suppliers', 'customers', 'products', 'orders', 'order_items']

    # Membuat dynamic task untuk setiap tabel
    for table in tables:
        etl = PythonOperator(
            task_id=f'extract_load_{table}',
            python_callable=etl_task,
            op_args=[table],
            dag=dag,
        )
        start_task >> etl  # Menjalankan task secara berurutan
