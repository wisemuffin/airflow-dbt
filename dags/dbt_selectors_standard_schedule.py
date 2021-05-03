from datetime import timedelta
from logging import raiseExceptions
from pathlib import Path
import pickle
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_dag_args = {
    "start_date": datetime(2020, 11, 24),
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "retries": 0,
}
DAG_NAME = "standard_schedule"
DBT_DIR = "/usr/local/airflow/data-cicd"
DBT_SELECTOR_PICKLE_DIR = "/usr/local/airflow/include/data"
GLOBAL_CLI_FLAGS = "--no-write-json"


dag = DAG(
    dag_id=f"dbt_{DAG_NAME}",
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
    default_args=default_dag_args,
)


def load_dag_def_pickle(file: str):
    """gets a pickled of depencies e.g. test 1 can only run after model run completes
        sharp: [(model1, test1),(test1, model2)]
    """
    data_folder = Path(DBT_SELECTOR_PICKLE_DIR)
    file_to_open = data_folder / file
    with file_to_open.open("rb") as f:
        objects = []
        while True:
            try:
                objects.append(pickle.load(f))
            except EOFError:
                break
    return objects[0]


def load_manifest():
    """loads the dbt manifest metadata file"""
    data_folder = Path(f"{DBT_DIR}", "target", "manifest.json")
    with open(data_folder) as f:
        data = json.load(f)
    return data


def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    model = node.split(".")[-1]
    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
            """,
            dag=dag,
        )
    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
            """,
            dag=dag,
        )

    else:
        raise Exception(
            f'dbt_verb in make_dbt_task is {dbt_verb} but can only be one of ["run","test"]'
        )

    return dbt_task


def create_task_dict(data, dag) -> dict:
    """
    Returns a dictionary of bash operators corresponding to dbt models/tests
    """
    dbt_tasks = {}

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            node_test = node.replace("model", "test")
            dbt_tasks[node] = make_dbt_task(node, "run")
            dbt_tasks[node_test] = make_dbt_task(node, "test")

    for node in data["nodes"].keys():
        if node.split(".")[0] == "model":
            # Set dependency to run tests on a model after model runs finishes
            node_test = node.replace("model", "test")
            dbt_tasks[node] >> dbt_tasks[node_test]
            # Set all model -> model dependencies
            for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
                upstream_node_type = upstream_node.split(".")[0]
                if upstream_node_type == "model":
                    dbt_tasks[upstream_node] >> dbt_tasks[node]
    return dbt_tasks


# Load dependencies from configuration file
dag_def = load_dag_def_pickle(f"{DAG_NAME}.pickle")

# Load the latest manifest file from DBT
manifest = load_manifest()

# Returns a dictionary of bash operators corresponding to dbt models/tests
dbt_tasks = create_task_dict(manifest, dag)

# Set dependencies between tasks according to config file
for edge in dag_def:
    dbt_tasks[edge[0]] >> dbt_tasks[edge[1]]
