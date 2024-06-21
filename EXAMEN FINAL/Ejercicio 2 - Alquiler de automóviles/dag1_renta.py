from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'German_Leventan'
}

with DAG(
    dag_id='Examen_Final_dag_padre',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['ingest']
) as dag:

    Comienzo = DummyOperator(
        task_id='Comienzo',
    )

    ingesta_datos = BashOperator(
        task_id='ingesta_datos',
        bash_command='ssh hadoop@172.17.0.2 /usr/bin/sh /home/hadoop/scripts/alquiler.sh ',
        dag=dag
    )

    trigger_dag_hijo = TriggerDagRunOperator(
        task_id='trigger_dag_hijo',
        trigger_dag_id='Examen_Final_dag_hijo',
        dag=dag
    )

    fin = DummyOperator(
        task_id='fin',
    )

    Comienzo >> ingesta_datos >> trigger_dag_hijo >> fin

if __name__ == "__main__":
    dag.cli()