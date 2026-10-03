# docker-airflow

This repository contains a **Docker Compose File** of [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/)'s [automated build](https://hub.docker.com/r/apache/airflow) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).

## Informations

* Based on Python (3.6-slim-buster) official Image [python:3.6-slim-buster](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

## Installation

Pull the image from the Docker repository.

    docker pull apache/airflow:2.0.0

## Usage

By default, docker-airflow runs Airflow with **LocalExecutor** :

    docker-compose -f docker-compose.yml up -d

NB : If you want to have DAGs example loaded (default=False), you've to set the following environment variable :

`AIRFLOW__CORE__LOAD_EXAMPLES`

in docker-compose.yml

If you want to use Ad hoc query, make sure you've configured connections:
Go to Admin -> Connections and Edit "postgres_default" set this values (equivalent to values in airflow.cfg/docker-compose*.yml) :
- Host : postgres
- Schema : airflow
- Login : postgres
- Password : postgres

## SAS macro migration DAGs

`dags/` contains Airflow DAGs that replicate the data-transformation macros from
`ts-sas-legacy-analytics/Macro/`: `sas_export_csv`, `sas_transpose`,
`sas_subset_data`, `sas_compare` and `sas_dedup_string`. Each runs
`extract >> <macro> >> validate`, where `validate` reconciles row counts and
checksums between input and output. Macro parameters can be overridden with
`dag_run.conf`, e.g.

    airflow dags trigger sas_subset_data -c '{"obs": "1-5 or 11-15", "keep": "_ALL_"}'

Run artefacts are written to `data/work/<dag_id>/<run_id>/`. Sample inputs live in
`data/sample/` (regenerate with `python data/sample/generate_samples.py <path to ts-sas-legacy-analytics/Data/csv>`).
Unit tests: `python -m pytest tests`. See [ETL_MIGRATION_NOTES.md](ETL_MIGRATION_NOTES.md) for translation decisions.

Enjoy!
