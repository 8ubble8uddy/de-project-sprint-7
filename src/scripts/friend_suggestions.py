# Импорты.
import os
import sys
import datetime as dt
import pytz
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


# Объявление main-функции.
def main():
        
        # Получение параметров джобы из аргументов командной строки.
        date = sys.argv[1]
        depth = int(sys.argv[2])
        events_base_path = sys.argv[3]
        user_locations_base_path = sys.argv[4]
        geo_base_path = sys.argv[5]
        output_base_path = sys.argv[6]

        # Создание сессии и именование джобы.
        spark = (
            SparkSession \
            .builder \
            .master('yarn') \
            .appName(f'FriendSuggestionsJob-{date}-d{depth}') \
            .getOrCreate()
        )

        # Чтение входных данных.
        events = spark.read.parquet(*input_paths(date, depth, events_base_path))
        user_locations = spark.read.parquet(f'{user_locations_base_path}/date={date}')
        geo = spark.read.csv(geo_base_path, header=True, sep=';')

        # Предобработка данных.
        subscriptions = events.where('event.subscription_channel IS NOT NULL').cache()
        messages = events.where('event.message_to IS NOT NULL').cache()
        users_geo = user_locations.join(geo.select('id', 'city'), F.col('act_city') == F.col('city') )

        # Вычисление выходного DataFrame.
        curr_locations = get_curr_locations(users_geo, messages)
        sub_neighbors = get_sub_neighbors(curr_locations, subscriptions)
        friend_suggestions = get_friend_suggestions(sub_neighbors, messages)
        friend_suggestions = friend_suggestions.withColumn('processed_dttm', F.lit(date))

        # Запись выходных данных.
        friend_suggestions.write.mode('overwrite').parquet(f'{output_base_path}/date={date}')



def input_paths(date_string: str, depth: int, basepath: str):
    date = dt.datetime.strptime(date_string, '%Y-%m-%d').date()
    paths = [
        f'{basepath}/date={date - dt.timedelta(days=num)}' for num in range(depth)
    ]
    return paths


def calculate_distance(lat1, lat2, lon1, lon2):
    radius_earth = 6371.0
    distance_in_kms =(
        F.round((F.acos((F.sin(F.radians(F.col(lat1))) * F.sin(F.radians(F.col(lat2)))) + \
           ((F.cos(F.radians(F.col(lat1))) * F.cos(F.radians(F.col(lat2)))) * \
            (F.cos(F.radians(lon1) - F.radians(lon2))))
               ) * F.lit(radius_earth)), 4)
    )
    return distance_in_kms


def get_curr_locations(users_geo, messages):
    curr_locations = (
        messages
        .groupBy(F.col('event.message_from').alias('user_id'))
        .agg(F.max_by('lat', 'event.message_ts').alias('lat'), F.max_by('lon', 'event.message_ts').alias('lon'))
        .join(users_geo, 'user_id', 'inner')
    )
    return curr_locations


def get_sub_neighbors(curr_locations , subscriptions):
    w = Window.partitionBy('act_city', 'channel')
    sub_locals = (
        subscriptions
        .selectExpr('event.user AS user_id', 'event.subscription_channel AS channel')
        .join(curr_locations, 'user_id', 'inner')
        .withColumn('channel_locals', F.collect_list(F.struct('user_id', 'lat', 'lon')).over(w))
        .selectExpr('*', 'INLINE(channel_locals) AS (sub_local, lat2, lon2)')
        .drop('channel', 'channel_locals')
        .distinct()
    )
    sub_neighbors = (
        sub_locals
        .withColumn('distance_in_kms', calculate_distance('lat', 'lat2', 'lon', 'lon2'))
        .filter((F.col('distance_in_kms') < 1) & (F.col('user_id') != F.col('sub_local')))
    )
    return sub_neighbors


def get_friend_suggestions(sub_neighbors, messages):
    contacts = (
        messages \
        .select('event.message_from', 'event.message_to') \
        .withColumn('user_id', F.expr('EXPLODE(ARRAY(message_from, message_to))')) \
        .withColumn('contact_id', F.expr('CASE WHEN message_from = user_id THEN message_to ELSE message_from END')) \
        .distinct()
    )
    friend_suggestions = (
        sub_neighbors
        .withColumnRenamed('sub_local', 'contact_id')
        .join(contacts, ['user_id', 'contact_id'], 'left_anti')
        .select(
            F.col('user_id').alias('user_left'),
            F.col('contact_id').alias('user_right'),
            F.col('id').alias('zone_id'),
            F.col('local_time')
        )
    )
    return friend_suggestions


# Вызов main-функции.
if __name__ == '__main__':
    main()
