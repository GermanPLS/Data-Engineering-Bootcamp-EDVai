from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago



args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='clase9-SQOOP-AIRFLOW',
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

	
with TaskGroup(
        group_id="ingest",
        tooltip="Ingest",
    ) as ingest_task_group:
            ingest_clientes = BashOperator(
            task_id='ingest_clientes',
            bash_command=(
                '/usr/bin/sh '
                '/home/hadoop/scripts/clientes.sh '
            )
        )

            ingest_envios = BashOperator(
            task_id='ingest_envios',
            bash_command=(
                '/usr/bin/sh '
                '/home/hadoop/scripts/pedidos.sh '
            )
        )

            ingest_order_details = BashOperator(
            task_id='ingest_order_details',
            bash_command='/usr/bin/sh /home/hadoop/scripts/detalles.sh ',
        )

with TaskGroup(
        group_id="transform",
        tooltip="Transform",
    ) as transform_task_group:
            transform_products_sold = BashOperator(
            task_id='transform_products_sold',
            bash_command=(
                "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit "
                "--files /home/hadoop/hive/conf/hive-site.xml "
                "/home/hadoop/scripts/clientes.py "
            )
        )

            transform_products_sent = BashOperator(
            task_id='transform_products_sent',
            bash_command=(
                "ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit "
                "--files /home/hadoop/hive/conf/hive-site.xml "
                "/home/hadoop/scripts/envio.py "
            )
        )

   
       finaliza_proceso = DummyOperator(task_id='finaliza_proceso')

  
comienza_proceso >> ingest >> transform >> finaliza_proceso




if __name__ == "__main__":
    dag.cli()
