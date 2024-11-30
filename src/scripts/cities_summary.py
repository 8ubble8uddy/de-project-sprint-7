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
        output_base_path = sys.argv[5] # '/user/germansizo/analytics/cities_summary_d10'

        # Создание сессии и именование джобы.
        spark = (
            SparkSession \
            .builder \
            .master('yarn') \
            .appName(f'CitiesSummaryJob-{date}-d{depth}') \
            .getOrCreate()
        )

        # Чтение входных данных.
        events = spark.read.parquet(*input_paths(date, depth, events_base_path))
        geo_raw = spark.read.csv(geo_base_path, header=True, sep=';')

        # Предобработка данных.
        geo = geo_raw.withColumns({'lat': F.regexp_replace('lat', ',', '.'), 'lon': F.regexp_replace('lng', ',', '.')})

        # Вычисление выходного DataFrame.
        city_events = get_city_events(geo, events)
        cities_summary = get_cities_summary(city_events)

        # Запись выходных данных.
        cities_summary.write.mode('overwrite').parquet(f'{output_base_path}/date={date}')



def input_paths(date_string: str, depth: int, basepath: str):
    date = dt.datetime.strptime(date_string, '%Y-%m-%d').date()
    paths = [
        f'{basepath}/date={date - dt.timedelta(days=num)}' for num in range(depth)
    ]
    return paths


def calculate_distance(lat1, lat2, lon1, lon2):
    distance_in_kms =(
        F.round((F.acos((F.sin(F.radians(F.col(lat1))) * F.sin(F.radians(F.col(lat2)))) + \
           ((F.cos(F.radians(F.col(lat1))) * F.cos(F.radians(F.col(lat2)))) * \
            (F.cos(F.radians(lon1) - F.radians(lon2))))
               ) * F.lit(6371.0)), 4)
    )
    return distance_in_kms


def get_city_events(geo, events):
    event_coordinates = events.select('lat', 'lon').distinct()
    city_events = (
        event_coordinates.alias('e')
        .crossJoin(geo.alias('g'))
        .withColumn('distance', calculate_distance('e.lat', 'g.lat', 'e.lon', 'g.lon'))
        .groupBy('e.lat', 'e.lon')
        .agg(F.min_by('g.id', 'distance').alias('zone_id'))
        .join(events, ['lat', 'lon'])
    )
    return city_events


def get_cities_summary(city_events):
    w1 = Window.partitionBy('event.message_from').orderBy('event.message_ts')
    w2 = Window.partitionBy('month', 'zone_id')
    cities_summary = (
        city_events
        .withColumn('dt', F.coalesce('event.message_ts', 'event.datetime'))
        .withColumn('registration', F.first('event.message_ts', ignorenulls=True).over(w1))
        .groupBy(F.month('dt').alias('month'), F.weekofyear('dt').alias('week'), 'zone_id')
        .agg(
            F.count('event.message_from').alias('week_message'),
            F.count('event.reaction_from').alias('week_reaction'),
            F.count('event.subscription_channel').alias('week_subscription'),
            F.count('registration').alias('week_user')
        ) \
        .withColumns({
            'month_message': F.sum('week_message').over(w2),
            'month_reaction': F.sum('week_reaction').over(w2),
            'month_subscription': F.sum('week_subscription').over(w2),
            'month_user': F.sum('week_user').over(w2)
        })
    )
    return cities_summary


# Вызов main-функции.
if __name__ == '__main__':
    main()
