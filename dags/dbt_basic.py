from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

DBT_PATH = "/usr/local/airflow/data-cicd"

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "astronomer",
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 23),
    "email": ["noreply@astronomer.io"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def checkIfRepoAlreadyCloned():
    if os.path.exists(f"{DBT_PATH}/.git"):
        return "dummy"
    return "git_clone"


with DAG(
    "dbt_basic_dag",
    default_args=default_args,
    description="An Airflow DAG to invoke D",
    schedule_interval=timedelta(days=1),
) as dag:

    t_git_clone = BashOperator(
        task_id="git_clone",
        bash_command=f"git clone https://github.com/wisemuffin/data-cicd.git {DBT_PATH}",
    )

    # Notice the trigger_rule sets to one_success
    # Why?
    # By default your tasks are set to all_success, so all parents must succeed for the task to be triggered
    # Here t_git_pull depends on either t_git_clone or t_dummy
    # By default if one these tasks is skipped then its downstream tasks will be skipped as well since the trigger_rule is set to all_succeed and so invalidate the task.
    # With one_success, t_git_pull will not be skipped since it now needs only either dummy or git_clone to succeed.
    t_git_pull = BashOperator(
        task_id="git_pull",
        bash_command=f"cd {DBT_PATH} && git pull",
        trigger_rule="one_success",
    )

    t_check_repo = BranchPythonOperator(
        task_id="is_repo_exists", python_callable=checkIfRepoAlreadyCloned
    )

    t_dummy = DummyOperator(task_id="dummy")

    t_check_repo >> t_git_clone
    t_check_repo >> t_dummy >> t_git_pull

    t_git_clone >> t_git_pull

    dbt_run = BashOperator(
        task_id="dbt_run", bash_command=f"cd {DBT_PATH} && dbt run", dag=dag,
    )

    dbt_test = BashOperator(
        task_id="dbt_test", bash_command=f"cd {DBT_PATH} && dbt test", dag=dag,
    )

    t_git_pull >> dbt_run >> dbt_test
