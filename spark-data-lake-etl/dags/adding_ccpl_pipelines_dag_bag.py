from airflow.models import DagBag

dag_dirs = ['~/<project>/dags', ]

for dir in dag_dirs:
    dag_bag = DagBag(dir)
    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag
