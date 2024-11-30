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




# events_partitioned = SparkSubmitOperator(
#     task_id='events_partitioned',
#     dag=dag_spark,
#     application ='/home/germansizo/partition_overwrite.py',
#     application_args = ['2022-05-25', '/user/master/data/events', '/user/germansizo/data/events']
# )

# verified_tags_candidates_d7 = SparkSubmitOperator(
#     task_id='verified_tags_candidates_d7',
#     dag=dag_spark,
#     application ='/home/germansizo/verified_tags_candidates.py',
#     application_args = ['2022-05-31', '7', '100', '/user/germansizo/data/events', '/user/master/data/snapshots/tags_verified/actual', '/user/germansizo/data/analytics/verified_tags_candidates_d7']
# )

# verified_tags_candidates_d84 = SparkSubmitOperator(
#     task_id='verified_tags_candidates_d84',
#     dag=dag_spark,
#     application ='/home/germansizo/verified_tags_candidates.py',
#     application_args = ['2022-05-31', '84', '1000', '/user/germansizo/data/events', '/user/master/data/snapshots/tags_verified/actual', '/user/germansizo/data/analytics/verified_tags_candidates_d84']
# )

# user_interests_d7 = SparkSubmitOperator(
#     task_id='user_interests_d7',
#     dag=dag_spark,
#     application ='/home/germansizo/user_interests.py',
#     application_args = ['2022-05-25', '7', '/user/germansizo/data/events', '/user/germansizo/analytics/user_interests_d7']
# )

# user_interests_d28 = SparkSubmitOperator(
#     task_id='user_interests_d28',
#     dag=dag_spark,
#     application ='/home/germansizo/user_interests.py',
#     application_args = ['2022-05-25', '28', '/user/germansizo/data/events', '/user/germansizo/analytics/user_interests_d28']
# )

# connection_interests_d7 = SparkSubmitOperator(
#     task_id='connection_interests_d7',
#     dag=dag_spark,
#     application ='/home/germansizo/connection_interests.py',
#     application_args = ['2022-05-25', '7', '/user/germansizo/data/events', '/user/germansizo/analytics/user_interests_d7', '/user/master/data/snapshots/tags_verified/actual', '/user/germansizo/data/analytics/connection_interests_d7'],
# )





# events_partitioned >> [verified_tags_candidates_d7, verified_tags_candidates_d84]
# events_partitioned >> [user_interests_d7, user_interests_d28]
# user_interests_d7 >> connection_interests_d7
# connection_interests_d7



# connection_interests_d71 = BashOperator(
#     task_id='connection_interests_d71',
#     bash_command='unset SPARK_HOME && /usr/local/bin/spark-submit --version ',
#     dag=dag_spark
# )

# connection_interests_d72 = BashOperator(
#     task_id='connection_interests_d72',
#     bash_command='/home/germansizo/run_jobs.sh ',
#     dag=dag_spark
# )

# connection_interests_d71 >> connection_interests_d72
