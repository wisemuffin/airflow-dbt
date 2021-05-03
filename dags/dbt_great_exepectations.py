import airflow
from airflow_dbt import DbtRunOperator
from airflow.operators.python import PythonOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

default_args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(1)}

dag = airflow.DAG(
    dag_id="sample_dag", default_args=default_args, schedule_interval=None,
)


def load_source_data():
    # Implement load to database
    pass


def publish_to_prod():
    # Implement load to production database
    pass


task_validate_source_data = GreatExpectationsOperator(
    task_id="validate_source_data", checkpoint_name="source_data.chk", dag=dag
)

task_load_source_data = PythonOperator(
    task_id="load_source_data", python_callable=load_source_data, dag=dag,
)

task_validate_source_data_load = GreatExpectationsOperator(
    task_id="validate_source_data_load", checkpoint_name="source_data_load.chk", dag=dag
)

task_run_dbt_dag = DbtRunOperator(task_id="run_dbt_dag", dag=dag)

task_validate_analytical_output = GreatExpectationsOperator(
    task_id="validate_analytical_output",
    checkpoint_name="analytical_output.chk",
    dag=dag,
)

task_publish = PythonOperator(
    task_id="publish", python_callable=publish_to_prod, dag=dag
)

task_validate_source_data.set_downstream(task_load_source_data)
task_load_source_data.set_downstream(task_validate_source_data_load)
task_validate_source_data_load.set_downstream(task_run_dbt_dag)
task_run_dbt_dag.set_downstream(task_validate_analytical_output)
task_validate_analytical_output.set_downstream(task_publish)
