"""
A DAG that demonstrates implementation of the GreatExpectationsOperator. 
Note: you wil need to reference the necessary data assets and expectations suites in your project. You can find samples available in the provider source directory.
To view steps on running this DAG, check out the Provider Readme: https://github.com/great-expectations/airflow-provider-great-expectations#examples
"""

import logging
import os
import airflow
from airflow import DAG
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

default_args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(1)}

dag = DAG(dag_id="example_great_expectations_dag", default_args=default_args)

# This runs an expectation suite against a sample data asset. You may need to change these paths if you do not have your `data`
# directory living in a top-level `include` directory. Ensure the checkpoint yml files have the correct path to the data file.
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_file = os.path.join(
    base_path, "include", "data/yellow_tripdata_sample_2019-01.csv"
)
ge_root_dir = os.path.join(base_path, "include", "great_expectations")


ge_batch_kwargs_pass = GreatExpectationsOperator(
    task_id="ge_batch_kwargs_pass",
    expectation_suite_name="taxi.demo",
    batch_kwargs={"path": data_file, "datasource": "data__dir"},
    data_context_root_dir=ge_root_dir,
    dag=dag,
)

# This runs an expectation suite against a data asset that passes the tests
ge_batch_kwargs_list_pass = GreatExpectationsOperator(
    task_id="ge_batch_kwargs_list_pass",
    assets_to_validate=[
        {
            "batch_kwargs": {"path": data_file, "datasource": "data__dir"},
            "expectation_suite_name": "taxi.demo",
        }
    ],
    data_context_root_dir=ge_root_dir,
    dag=dag,
)

# This runs a checkpoint and passes in a root dir.
ge_checkpoint_pass_root_dir = GreatExpectationsOperator(
    task_id="ge_checkpoint_pass_root_dir",
    run_name="ge_airflow_run",
    checkpoint_name="taxi.pass.chk",
    data_context_root_dir=ge_root_dir,
    dag=dag,
)

# This runs a checkpoint that will pass. Make sure the checkpoint yml file has the correct path to the data file.
ge_checkpoint_pass = GreatExpectationsOperator(
    task_id="ge_checkpoint_pass",
    run_name="ge_airflow_run",
    checkpoint_name="taxi.pass.chk",
    data_context_root_dir=ge_root_dir,
    dag=dag,
)

# This runs a checkpoint that will fail. Make sure the checkpoint yml file has the correct path to the data file.
ge_checkpoint_fail = GreatExpectationsOperator(
    task_id="ge_checkpoint_fail",
    run_name="ge_airflow_run",
    checkpoint_name="taxi.fail.chk",
    data_context_root_dir=ge_root_dir,
    dag=dag,
)

# This runs a checkpoint that will fail, but we set a flag to exit the task successfully.
ge_checkpoint_fail_but_continue = GreatExpectationsOperator(
    task_id="ge_checkpoint_fail_but_continue",
    run_name="ge_airflow_run",
    checkpoint_name="taxi.fail.chk",
    fail_task_on_validation_failure=False,
    data_context_root_dir=ge_root_dir,
    dag=dag,
)


ge_batch_kwargs_list_pass >> ge_checkpoint_pass_root_dir >> ge_batch_kwargs_pass >> ge_checkpoint_fail_but_continue >> ge_checkpoint_pass >> ge_checkpoint_fail

