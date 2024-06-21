
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='Ejercicio_1_Aviacion_Civil',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:

    comienza_proceso = DummyOperator(
        task_id='comienza',
    )
	

    finaliza_proceso = DummyOperator(
        task_id='finaliza',
    )

    ingest = BashOperator(
        task_id='ingest',
        bash_command='/usr/bin/sh /home/hadoop/scripts/aviacion.sh ',
    )

    with TaskGroup('Transform') as Transform:
        transform_drivers = BashOperator(
        task_id='transform_tabla1',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_tabla1.py ',
    )
        transform_constructors = BashOperator(
        task_id='transform_tabla2',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_tabla2.py ',
    )
  

    comienza_proceso >> ingest >> Transform >> finaliza_proceso


if __name__ == "__main__":
    dag.cli()