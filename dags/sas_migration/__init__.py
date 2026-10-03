"""Python/pandas equivalents of the legacy SAS macro library (ts-sas-legacy-analytics/Macro).

The package is framework-agnostic: every transform is a pure function over pandas
DataFrames so it can be unit tested without Airflow. The DAG files in the parent
``dags/`` folder wire these functions into PythonOperator tasks.

Code must stay Python 3.6 compatible (apache/airflow:2.0.0 image).
"""
