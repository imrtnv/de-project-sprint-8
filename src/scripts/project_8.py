from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
from pyspark.sql import functions as f

from config import kafka_security_options, docker_postgresql_settings, postgresql_settings, spark_jars_packages, TOPIC_IN, TOPIC_OUT

from datetime import datetime
from time import sleep

# Инициализация spark сессии с библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
def spark_init(name_connect) -> SparkSession:
    spark = (
        SparkSession.builder.appName(name_connect)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
    )
    return spark

# Получение текущего времени
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# Читаем данные с Kafka сообщения с акциями от ресторанов 
def read_adv_stream(spark: SparkSession) -> DataFrame:
    # Задаем схему
    schema = StructType([
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType()), True,
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", DoubleType(), True),
        StructField("adv_campaign_datetime_end", DoubleType(), True),
        StructField("datetime_created", DoubleType(), True)
    ])
    # Поулчение данных
    restaurant_read_stream_df = (spark.readStream \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('subscribe', TOPIC_IN) \
        .load()
        .withColumn('value', f.col('value').cast(StringType()))
        .withColumn('events', f.from_json(f.col('value'), schema))
        .selectExpr('events.*')
        .where((f.col("adv_campaign_datetime_start") < current_timestamp_utc) & (f.col("adv_campaign_datetime_end") > current_timestamp_utc))
    )
    return restaurant_read_stream_df

# Получаем данные всех пользователей с подпиской на рестораны
def read_user(spark: SparkSession) -> DataFrame:
    subscribers_restaurant_df = (spark.read
                    .format("jdbc")
                    .options(**postgresql_settings)
                    .load()
    )
    return subscribers_restaurant_df



# Джоиним стрим с акциями и статическую таблицу с юзерами с подпиской
def join(restaurant_read_stream_df, subscribers_restaurant_df) -> DataFrame:
    join_df = restaurant_read_stream_df \
    .join(subscribers_restaurant_df, 'restaurant_id', 'inner') \
    .withColumn('trigger_datetime_created', f.lit(current_timestamp_utc)) \
    .select(
        'restaurant_id',
        'adv_campaign_id',
        'adv_campaign_content',
        'adv_campaign_owner',
        'adv_campaign_owner_contact',
        'adv_campaign_datetime_start',
        'adv_campaign_datetime_end',
        'datetime_created',
        'client_id',
        'trigger_datetime_created'
    )
    return join_df

# создаем выходные сообщения с фидбеком
def foreach_batch_function(df, epoch_id):
        # Кэш датафрейма df в памяти
        df.persist()

        feedback_df = df.withColumn('feedback', f.lit(None).cast(StringType()))

        # Запись данных
        feedback_df.write.format('jdbc')\
            .mode('append') \
            .options(** docker_postgresql_settings) \
            .save()

        df_to_stream = (feedback_df
                    .select(f.to_json(f.struct(f.col('*'))).alias('value'))
                    .select('value')
                    )
        # Запись в kafka
        df_to_stream.write \
            .format('kafka') \
            .options(**kafka_security_options) \
            .option('topic', TOPIC_OUT) \
            .option('truncate', False) \
            .save()

        # Удаление из кеша
        df.unpersist()


if __name__ == "__main__":
    spark = spark_init('adv_Restaurant_campaign_for_user')
    adv_stream = read_adv_stream(spark)
    user_df = read_user(spark)
    output = join(adv_stream, user_df)

    query = (output.writeStream \
        .foreachBatch(foreach_batch_function) \
        .start())
    try:
        query.awaitTermination()
    finally:
        query.stop()