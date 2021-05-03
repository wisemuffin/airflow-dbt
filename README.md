# Airflow DAGs for dbt

> The code in this repository is meant to accompany [this blog post](https://astronomer.io/blog) on beginner and advanced implementation concepts at the intersection of dbt and Airflow.

To run these DAGs locally:
1. Download the [Astro CLI](https://github.com/astronomer/astro-cli)
2. Download and run [Docker](https://docs.docker.com/docker-for-mac/install/)
3. Clone this repository and `cd` into it.
4. Run `astro dev start` to spin up a local Airflow environment and run the accompanying DAGs on your machine.

We are currently using a [sample `manifest.json`](https://github.com/fishtown-analytics/dbt-docs/blob/master/data/manifest.json) file pulled from the dbt docs, but if you would like to try these dags with your own dbt workflow, feel free to copy and paste your `manifest.json` file into the `dags/dbt/target/` directory.

## dbt_selectors_standard_schedule
This DAG recieves all the DBT tasks based on DBT selectors [docs](https://www.astronomer.io/blog/airflow-dbt-2)

CICD Tool
- load manifest
- generate_all_model_dependencies
- pickle

Airflow DAG
- loads pickle file based on selectors
- generate DBT tasks based on selectors

### CICD pickle manifest

This lives in "/home/dave/data-engineering/data-cicd/.github/workflows/dependency_graph.py"

Currently pickly file written out to "/home/dave/data-engineering/data-cicd/dbt_dags/data/*"

This file is a utility script that is run via CircleCI in the deploy
step. It is not run via Airflow in any way. The point of this script is
to generate a pickle file that contains all of the dependencies between dbt models
for each dag (usually corresponding to a different schedule) that we want
to run.

### TODO dbt_selectors_standard_schedule
- current manifest.json and depencies pickle live in include/data/ will need to move them into s3

