from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='Ingest-Hive-Titanic',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:

    comienza_proceso = DummyOperator(
        task_id='comenzar_proceso',
    )


    ingest_Hive = BashOperator(
        task_id='ingest_Hive',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/edades.py',
    )


    
    finaliza_proceso = DummyOperator(
        task_id='finalizar_proceso',
    )

comienza_proceso >> ingest_Hive  >>  finaliza_proceso


if __name__ == "__main__":
    dag.cli()