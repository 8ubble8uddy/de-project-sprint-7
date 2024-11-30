import os
from datetime import date, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['HOME'] = '/home/germansizo'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'params': {
        'date': '2022-05-22',
        'depth': '10',
        'events_base_path': '/user/master/data/geo/events',
        'geo_base_path': '/user/germansizo/data/geo.csv',
    }
}

dag_bash = DAG(
    dag_id = 'dag_bash',
    default_args=default_args,
    schedule_interval=None,
)

user_locations = BashOperator(
    task_id='user_locations',
    bash_command='unset SPARK_HOME && spark-submit --master yarn --deploy-mode cluster \
                        {{ params.application }} \
                        {{ params.date }} \
                        {{ params.depth }} \
                        {{ params.events_base_path }} \
                        {{ params.geo_base_path }} \
                        {{ params.output_base_path }} ',
    params={
        'application': '/home/germansizo/user_locations.py',
        'output_base_path': '/user/germansizo/analytics/user_locations'
    },
    dag=dag_bash
)

cities_summary = BashOperator(
    task_id='cities_summary',
    bash_command='unset SPARK_HOME && spark-submit --master yarn --deploy-mode cluster \
                        {{ params.application }} \
                        {{ params.date }} \
                        {{ params.depth }} \
                        {{ params.events_base_path }} \
                        {{ params.geo_base_path }} \
                        {{ params.output_base_path }} ',
    params={
        'application': '/home/germansizo/cities_summary.py',
        'output_base_path': '/user/germansizo/analytics/cities_summary'
    },
    dag=dag_bash
)

friend_suggestions = BashOperator(
    task_id='friend_suggestions',
    bash_command='unset SPARK_HOME && spark-submit --master yarn --deploy-mode cluster \
                        {{ params.application }} \
                        {{ params.date }} \
                        {{ params.depth }} \
                        {{ params.events_base_path }} \
                        {{ params.user_locations_base_path }} \
                        {{ params.geo_base_path }} \
                        {{ params.output_base_path }} ',
    params={
        'application': '/home/germansizo/friend_suggestions.py',
        'user_locations_base_path': '/user/germansizo/analytics/user_locations',
        'output_base_path': '/user/germansizo/analytics/friend_suggestions'
    },
    dag=dag_bash
)


user_locations >> friend_suggestions
cities_summary
