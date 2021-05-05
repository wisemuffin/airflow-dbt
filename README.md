# Airflow Setup

To run these DAGs locally:
1. Download the [Astro CLI](https://github.com/astronomer/astro-cli)
2. Download and run [Docker](https://docs.docker.com/docker-for-mac/install/)
3. Clone this repository and `cd` into it.
4. Run `astro dev start` to spin up a local Airflow environment and run the accompanying DAGs on your machine.


## additional config
- once airflow is running, add a dbt profile.yml to /home/astro/.dbt/profiles.yml
- dbt's manifest.json needs to be built each time in airflow docker: /usr/local/airflow/data-cicd/target

# DAGS

> The DBT DAGs in this repository are built ontop of [this blog post](https://astronomer.io/blog) on beginner and advanced implementation concepts at the intersection of dbt and Airflow.

## dbt_basic
- runs some conditional logic to clone dbt repo
- runs dbt without split each dbt model's build

## dbt_advanced
- uses the manifest.json to build out depencies as individual Airflow DAG tasks. Giving greater visability on errors, and bringing retry logic to dbt.

## dbt_selectors_standard_schedule
This DAG receives all the DBT tasks based on DBT selectors [docs](https://www.astronomer.io/blog/airflow-dbt-2) this allows us to take dbt_advanced and break out the DBT model based on selectors. The benefit for this is if we need to run DAGs at different intervals and times.

CICD Tool
- load manifest
- generate_all_model_dependencies
- pickle

Airflow DAG
- loads pickle file based on selectors
- generate DBT tasks based on selectors

### CICD pickle manifest

This lives in "/home/dave/data-engineering/data-cicd/.github/workflows/dependency_graph.py"

Currently pickle file written out to "/home/dave/data-engineering/data-cicd/dbt_dags/data/*"

This file is a utility script that is run via CircleCI in the deploy
step. It is not run via Airflow in any way. The point of this script is
to generate a pickle file that contains all of the dependencies between dbt models
for each dag (usually corresponding to a different schedule) that we want
to run.

### TODO dbt_selectors_standard_schedule
- current depencies pickle live in include/data/ will need to move them into s3

## great_expectations
example of using great expectations with local data

## dbt_great_expectations
NOT working
provides boiler plate for DBT with great expectations