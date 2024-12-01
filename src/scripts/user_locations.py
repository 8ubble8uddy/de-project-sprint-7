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
        geo_base_path = sys.argv[4]
        output_base_path = sys.argv[5]

        # Создание сессии и именование джобы.
        spark = (
            SparkSession \
            .builder \
            .master('yarn') \
            .appName(f'UserLocationsJob-{date}-d{depth}') \
            .getOrCreate()
        )

        # Чтение входных данных.
        events = spark.read.parquet(*input_paths(date, depth, events_base_path))
        geo_raw = spark.read.csv(geo_base_path, header=True, sep=';')

        # Предобработка данных.
        messages = events.where('event.message_to is not null')
        geo = geo_raw.withColumns({'lat': F.regexp_replace('lat', ',', '.'), 'lon': F.regexp_replace('lng', ',', '.')})

        # Вычисление выходного DataFrame.
        cities = get_cities_geo(geo)
        city_messages = get_city_messages(cities, messages)
        user_locations = get_user_locations(city_messages)

        # Запись выходных данных.
        user_locations.write.mode('overwrite').parquet(f'{output_base_path}/date={date}')


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


def get_cities_geo(geo):
    tz_geo = (
        geo
        .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('city')))
        .filter(F.col('timezone').isin(pytz.all_timezones))
    )
    cities = (
        geo.alias('g').crossJoin(tz_geo.alias('t'))
        .withColumn('distance', calculate_distance('g.lat', 't.lat', 'g.lon', 't.lon'))
        .groupBy('g.id', 'g.city', 'g.lat', 'g.lon')
        .agg(F.min_by('timezone', 'distance').alias('timezone'))
    )
    return cities


def get_city_messages(cities, messages):
    message_coordinates = messages.select('lat', 'lon').distinct()
    city_messages = (
        message_coordinates.alias('m')
        .crossJoin(cities.alias('c'))
        .withColumn('distance', calculate_distance('m.lat', 'c.lat', 'm.lon', 'c.lon'))
        .groupBy('m.lat', 'm.lon')
        .agg(F.min_by('c.city', 'distance').alias('city'), F.min_by('c.timezone', 'distance').alias('timezone'))
        .join(messages, ['lat', 'lon'], 'inner')
    )
    return city_messages


def get_user_locations(city_messages):
    w = Window.partitionBy('user_id').orderBy('ts')
    users_locations = (
        city_messages
        .selectExpr('event.message_from AS user_id', 'event.message_ts AS ts', 'city', 'timezone')
        .withColumn('last_ts', F.last('ts').over(w.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        .withColumn('start_streak', F.when(F.col('city') != F.lag('city', 1, 'dummy').over(w), F.col('ts')))
        .filter(F.col('start_streak').isNotNull())
        .withColumn('end_streak', F.coalesce(F.lead('start_streak', 1).over(w), F.col('last_ts')))
        .withColumn('cnt_streak', F.date_diff('end_streak', 'start_streak'))
        .groupBy('user_id') \
        .agg(
            F.max_by('city', 'last_ts').alias('act_city'),
            F.max_by('city', F.when(F.col('cnt_streak') >= 27, F.col('ts'))).alias('home_city'),
            F.count('*').alias('travel_count'),
            F.sort_array(F.collect_list(F.struct('ts', 'city')))['city'].alias('travel_array'),
            F.from_utc_timestamp(F.max('last_ts'), F.max_by('timezone', 'last_ts')).alias('local_time')
        )   
    )
    return users_locations


# Вызов main-функции.
if __name__ == '__main__':
    main()    
