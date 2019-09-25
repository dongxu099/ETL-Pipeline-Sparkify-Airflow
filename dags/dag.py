from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from dim_subdag import dim_load_dag

default_args = {
    'owner': 'dx',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('dag-reborn-11',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

table_creation = PostgresOperator(
    task_id='tables_creation',  
    dag=dag,
    postgres_conn_id='redshift',
    sql = '/create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key = "log-data/{execution_date.year}/{execution_date.month:02d}",    
    table="staging_events",
    file_format='JSON \'s3://udacity-dend/log_json_path.json\''
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    conn_id='redshift',
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key = 'song_data/A/A',
    file_format = 'JSON \'auto\'' 
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    conn_id='redshift',
    sql=SqlQueries.songplay_table_insert    
)

load_dim_subdag = SubDagOperator(    
    subdag=dim_load_dag(
        parent_dag_name="dag-reborn-11",
        task_id="load_dimensions_subdag",
        conn_id="redshift",
        tables=({'table':'users', 'sql':SqlQueries.user_table_insert},
                {'table':'songs', 'sql':SqlQueries.song_table_insert},
                {'table':'artists', 'sql':SqlQueries.artist_table_insert},
                {'table':'times', 'sql':SqlQueries.time_table_insert}),
        start_date=default_args['start_date'],
    ),
    task_id="load_dimensions_subdag",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    queries=({"table": "times","where":"start_time IS NULL","result":0},
             {"table": "songs","where":"songid IS NULL","result":0},
             {"table": "songplays","result":20460})    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> table_creation
table_creation >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_dim_subdag
load_dim_subdag >> run_quality_checks 
run_quality_checks >> end_operator
