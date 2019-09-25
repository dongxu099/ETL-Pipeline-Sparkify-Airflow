from airflow import DAG
from airflow.operators import LoadDimensionOperator

def dim_load_dag(
    parent_dag_name,
    task_id,
    conn_id,
    tables,
    *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}", **kwargs)
    
    with dag:
        for table in tables:
            task_id = f"load_{table.get('table')}_dim_table"
            load_data = LoadDimensionOperator(
                task_id=task_id,
                dag=dag,
                conn_id=conn_id,
                table=table.get("table"),
                sql=table.get("sql"),
                append_only=False)

    return dag