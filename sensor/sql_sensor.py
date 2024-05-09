from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import psycopg2 as ps

# DEFINE YOUR DATABASE INFORMATION HERE
db_host="HOST_ADDRESS" #Important note: use host.docker.internal if you are running airflow on docker
db_port="PORT"
db_user="USER"
db_password="PASSWORD"
db_name="DB_NAME"

# DEFINE YOUR TABLE NAME AND THE DATE COLUMN YOU WANT TO CHECK
TABLE="ENTER_YOUR_TABLE_NAME_HERE"
DATE_COL="ENTER_YOUR_COLUMN_NAME_HERE"

# queries which will be executed by code , change them if you want
max_val_query="SELECT MAX({date_column}) FROM {table_name}"
sensor_query="SELECT COUNT(*) FROM {table_name} WHERE {date_column} > '{{ ti.xcom_pull(task_ids='find_last_date', key='max_value') }}'"
operator_query="SELECT * FROM {table_name} WHERE {date_column} > '{{ ti.xcom_pull(task_ids='find_last_date', key='max_value') }}'"

# defining your database connection ID which you defined in airflow connection tab
CONN_ID="ENTER_YOUR_CONN_ID"


def find_max_val(**kwargs):
    conn=ps.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=db_name)
    cur=conn.cursor()

    query=max_val_query.format(table_name=TABLE,date_column=DATE_COL)
    cur.execute(query)
    max_date=cur.fetchone()
    
    cur.close()
    conn.close()
    
    max_value=str(max_date[0])
    kwargs['ti'].xcom_push(key='max_value', value=max_value)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, # how many times your dag should retry a task if it fails
    'retry_delay': timedelta(seconds=5), #retry time frequency
}

dag = DAG(
    'check_table_insert', #name of your dag which you can find in airflow Ui
    default_args=default_args,
    description='Check if a record is inserted into table',
    schedule_interval=timedelta(seconds=1), #, dag will run again right after each run is finished (succesfull or failed)
    max_active_runs=1,
    catchup=False,
)

# making an operator to find date of latest row inserted into the table
find_max_date=PythonOperator(
    task_id="find_last_date",
    python_callable=find_max_val,
    provide_context=True,
    dag=dag,
)


# Sensor to check if a new row is inserted into the table
shop_insert_sensor = SqlSensor(
    task_id='table_insert_sensor',
    conn_id=CONN_ID,
    sql=sensor_query.format(table_name=TABLE,date_column=DATE_COL),
    mode='poke',
    poke_interval=timedelta(seconds=1),#your sesnor check database every secnod until it finds a new row
    timeout=6000,
    dag=dag,
)

# operator to read the new rows which added to your table
read_row_from_db=PostgresOperator(
    task_id="read_data_from_db",
    postgres_conn_id=CONN_ID,
    sql=operator_query.format(table_name=TABLE,date_column=DATE_COL),
    do_xcom_push=True,
    dag=dag,
)



def write_result(**context):
    # write new rows into a text file in your dags folder (where you mounted your dag folder in .yaml file)
    with open("/opt/airflow/dags/result.txt","a") as s: 
        data = context["ti"].xcom_pull(task_ids='read_data_from_db')
        text="added on:"+str(datetime.now())+"\n" 
        final= str(data) + " => " + text 
        s.write(final)


check_table_insert = PythonOperator(
    task_id='check_table_insert_task',
    python_callable=write_result,
    dag=dag,
)


# Setting the task dependencies
find_max_date >> shop_insert_sensor >> read_row_from_db >> check_table_insert