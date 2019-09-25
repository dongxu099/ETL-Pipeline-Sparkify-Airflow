# Summary
This project builds a data pipeline using Apache Airflow and AWS Redshift in the analytics context of the startup called Sparkify. Specifically, the repository presents:

* DAGs
* Plugins

To create such data pipeline, the general procedures are

## STEP 0: Create working directory hierarchy

*	plugins/operators/__init__.py, custom_operator.py
*	plugins/helpers/__init__.py, helper.py
*	dags/dag.py
*	dags/subdag.py
*	create_tables.sql

## STEP 1: Define plugins (custom operators)

*	Check existing hooks in https://github.com/apache/airflow/tree/master/airflow/hooks
*	Read data operators, including hooks
*	ETL operators
*	Data quality checks operators

## STEP 2: A helper.py for ETL (including SQL transformations)

*	If the SQL statement is not listed here, it can also be listed to the custom operator

## STEP 3: Create a DAG template

*	Import operators
*	Import helpers
*	Import DAG
*	Import subdags
*	List default arguments
*	DAG
*	Configure the DAG
*	Define functions for tasks: basic operators using predefined functions + custom operators using plugins+subdags
*	task dependencies

## STEP 4: Create the working cluster

## STEP 5: Back to Web Server

*	connect to working server

## STEP 6: Launch working server
## STEP 7: Toggle the DAG “ON” – Click “Play”
## STEP 8: Use Udacity Workspace to access Airflow

*	$ /opt/airflow/start.sh ##start the webserver
*	Click “ACCESS AIRFLOW”

## STEP 9: Airflow-AWS Connection Configuration

*	Admin – Connections – Create – AWS_credentials
*	Admin – Connections – Create – Postgre

## STEP 10: Close the working cluster



