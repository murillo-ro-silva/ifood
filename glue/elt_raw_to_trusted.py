import sys
import logging
import datetime as dt

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
import pyspark.sql.functions as sf
import pyspark.sql.types as st
import pyspark.sql as s



## @params: [JOB_NAME, env]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'env'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
ENV = args['env']


def config_log():
    from sys import stdout
    stdout_handler = logging.StreamHandler(stdout)
    handlers = [stdout_handler]
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=handlers
    )
    return logging.getLogger('JOB')

logger = config_log()


def datetime_result(date_type):
    """ Recover brazilian time.
    """ 
    if date_type == 'today':
        date_now = (dt.datetime.utcnow() - dt.timedelta(hours=3)).strftime('%Y-%m-%d')
        return str(date_now)
    if date_type == 'today_timestamp':
        date_now = (dt.datetime.utcnow() - dt.timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
        return str(date_now)
    else:
        date_yesternoon = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
        return str(date_yesternoon)


def hash_column(df, columns):
    """
    Used to hash columns
    :param df: :class:`pyspark.sql.DataFrame` instance
    :param columns: list, columns to hash
    :return df: :class:`pyspark.sql.DataFrame` instance
    """
    salt = 'QglW6kADoUt8-FXIHQQsj3tK-Vpz6QaZ2DoCCQKEARM='
    for column in columns:
        df = df.withColumn(column, sf.trim(sf.col(column)))
        df = df.withColumn(column, sf.concat(column, sf.lit(salt)))
        df = df.withColumn(column, sf.sha2(column, 256))
    return df


@sf.udf
def hash_tel(column):
    """
    Def for anonymized phone
    """
    return int(column) * 5 / 2


def path_read_write(zone, dataset_name):
    """
    Def mount absolute bucket path.
    """
    BASE_PATH_SOURCE = 's3://ifood-data-architect-test-source-murillo'

    PATH = BASE_PATH_SOURCE + '/' + zone + '/' + dataset_name

    return PATH


def dedup_dataframe(pk_col, max_col, dataframe):
    """
    Def for deduplication remaining the most recent record
    """
    LAST_VALUE_ORDER = s.Window.partitionBy(
        pk_col) \
        .orderBy(sf.col(max_col).desc())

    DF_DEDUP = dataframe.dropDuplicates() \
        .withColumn("distinct", sf.row_number().over(LAST_VALUE_ORDER)) \
        .filter("distinct = 1") \
        .drop("distinct")

    return DF_DEDUP


def checking_quality(actual, previous):
    """
    :param actual: number of records comming from last trusted zone.
    :param previous: number of records comming from the wildcard previues trusted zone.
    :return: TRUE, if the dataset is OK to be inserted / FALSE, if any problem
    related to quality was found.
    """
    # Checking the percentage of drop in the counting
    maximum_percentage_of_drop_in_count = 80

    logger.info(f"Quality Message: Actual {actual} - Previous {previous}")
    
    if actual < previous:
        if ((((actual / previous) - 1) * 100) * -1) >= maximum_percentage_of_drop_in_count:  # noqa
            return False

    return True


def write_trusted(dataframe, repartition, partition, dataset):
    
    PATH = path_read_write("trusted", dataset)
    try:
        # Todo, find last dataset wildcard using boto3 for figureout that.
        DF_LATEST = spark.read \
            .parquet(PATH + '/partition=' + datetime_result("yesterday"))
        latest_v2_count = DF_LATEST.count()
    
    except Exception:
        latest_v2_count = 1
    
    logger.info("Getting the count from the current versions of the trusted")
    latest_count = dataframe.count()
    quality_check = checking_quality(latest_count, latest_v2_count)
    
    if quality_check:
        
        dataframe.repartition(repartition).write \
        .parquet(PATH + '/partition=' + datetime_result("today"),
             mode="overwrite",
             partitionBy=partition)
    else:
        logger.info(f"Quality Urgent: Dataset: {dataset} are 80% lower. Check it out!!")


# Setting for read all datasets in raw zone
RAW_ORDER = spark.read \
    .load(path_read_write("raw", "order"), format='parquet') \
    .drop("partition_column", "customer_name") \

RAW_STATUS = spark.read \
    .load(path_read_write("raw", "status"), format='parquet') \
    .drop("partition_column") \
    .withColumnRenamed("created_at", "status_created_at") \
    .withColumnRenamed("value", "status_value") \

RAW_CONSUMER = spark.read \
    .load(path_read_write("raw", "consumer"), format='parquet') \
    .drop("partition_column", "customer_name") \
    .withColumnRenamed("created_at", "consumer_created_at") \
    .withColumn("customer_phone_number", hash_tel('customer_phone_number'))

RAW_RESTAURANT = spark.read \
    .load(path_read_write("raw", "restaurant"), format='parquet') \
    .drop("partition_column") \
    .withColumnRenamed("created_at", "restaurant_created_at")


def trusted_order():

    # Join Order + Consumer + Restaurant
    DF_PRIMARY = RAW_ORDER \
        .join(RAW_CONSUMER, 'customer_id') \
        .join(RAW_RESTAURANT, RAW_RESTAURANT.id == RAW_ORDER.merchant_id)

    # Read Order dataset
    DF_TRUSTED_ORDER = dedup_dataframe(
        "customer_id",
        "order_created_at",
        DF_PRIMARY)

    # Read Status dataset
    DF_TRUSTED_STATUS = dedup_dataframe(
        "order_id",
        "status_created_at",
        RAW_STATUS) \
        .select("order_id", "status_created_at", "status_value")

    # Joining
    DF_TRUSTED_FINAL = DF_TRUSTED_ORDER \
        .join(DF_TRUSTED_STATUS, "order_id") \
        .withColumn("partition",
                    sf.lit(datetime_result("today")))

    DF_TRUSTED_ANONYMIZED = hash_column(DF_TRUSTED_FINAL, ['cpf'])

    # Writing DF in trusted zone.
    write_trusted(DF_TRUSTED_ANONYMIZED, 30, "partition", "order")


def trusted_order_items():

    PATH_ORDER = path_read_write("trusted", "order")

    DF_TRUSTED = spark.read \
        .parquet(PATH_ORDER)

    # Recovering dynamic schema
    JSON_SCHEMA = spark.read \
        .json(DF_TRUSTED.rdd.map(lambda row: row.items)).schema

    # Inferring schema in string column, to become array para usar explode code
    JSON_SCHEMA = st.ArrayType(JSON_SCHEMA)
    DF_FLATTEN = DF_TRUSTED \
        .withColumn("items", sf.from_json("items", JSON_SCHEMA))

    # Exploding and struturing Items for construct Order Items
    DF_EXPLODE_ITEMS = DF_FLATTEN.select(
        "order_id",
        sf.explode("items").alias("items"))

    # Structuring data
    DF_EXPLODE_ITEMS_DETAILS = DF_EXPLODE_ITEMS \
        .select("order_id",
                sf.col("items.name").alias("name"),
                sf.col("items.addition")
                .getItem('currency').alias("addition_currency"),
                sf.col("items.addition").getItem('value').alias("addition"),
                sf.col("items.discount")
                .getItem('currency').alias("discount_currency"),
                sf.col("items.discount").getItem('value').alias("discount"),
                sf.col("items.quantity").alias("quantity"),
                sf.col("items.unitPrice")
                .getItem('currency').alias("unit_price_currency"),
                sf.col("items.unitPrice").getItem('value').alias("unit_price"),
                sf.col("items.externalId").alias("external_id"),
                sf.col("items.totalValue")
                .getItem('currency').alias("total_value_currency"),
                sf.col("items.totalValue")
                .getItem('value').alias("total_value"),
                sf.col("items.customerNote").alias("customer_note"),
                sf.col("items.integrationId").alias("integration_id"),
                sf.col("items.totalAddition")
                .getItem('currency').alias("tota_addition_currency"),
                sf.col("items.totalAddition")
                .getItem('value').alias("tota_addition"),
                sf.col("items.totalDiscount")
                .getItem('currency').alias("total_discount_currency"),
                sf.col("items.totalDiscount")
                .getItem('value').alias("total_discount"),
                sf.explode("items.garnishItems.externalId")
                .alias("garnish_external_id"),
                sf.col("items.garnishItems").alias("garnish_items")
                ) \
        .dropDuplicates()

    # Exploding and struturing Garnish for additing Order Items
    DF_EXPLODE_GARNISH = DF_EXPLODE_ITEMS_DETAILS.select(
        "order_id",
        "garnish_external_id",
        sf.explode("garnish_items").alias("garnish_items"))

    # Structuring data
    DF_EXPLODE_GARNISH_DETAIL = DF_EXPLODE_GARNISH \
        .select("order_id",
                sf.col("garnish_items.name").alias("garnish_name"),
                sf.col("garnish_items.addition")
                .getItem('value').alias("garnish_addition"),
                sf.col("garnish_items.addition")
                .getItem('currency').alias("garnish_addition_currency"),
                sf.col("garnish_items.discount")
                .getItem('value').alias("garnish_discount"),
                sf.col("garnish_items.discount")
                .getItem('currency').alias("garnish_discount_currency"),
                sf.col("garnish_items.quantity").alias("garnish_quantity"),
                sf.col("garnish_items.sequence").alias("garnish_sequence"),
                sf.col("garnish_items.unitPrice")
                .getItem('value').alias("garnish_unit_price"),
                sf.col("garnish_items.unitPrice")
                .getItem('currency').alias("garnish_unit_price_currency"),
                sf.col("garnish_items.categoryId")
                .alias("garnish_category_id"),
                sf.col("garnish_items.categoryName")
                .alias("garnish_category_name"),
                sf.col("garnish_items.externalId")
                .alias("garnish_external_id"),
                sf.col("garnish_items.totalValue")
                .getItem('value').alias("garnish_total_value"),
                sf.col("garnish_items.totalValue")
                .getItem('currency').alias("garnish_total_value_currency"),
                sf.col("garnish_items.integrationId")
                .alias("garnish_integration_id"),
                ) \
        .dropDuplicates()

    # Union itens with garnish dataframe,
    # when 1 or more item can have 1 or more garnish.
    DF_UNION_ITENS_GARNISH = DF_EXPLODE_ITEMS_DETAILS \
        .join(DF_EXPLODE_GARNISH_DETAIL, ["order_id", "garnish_external_id"]) \
        .drop("garnish_items") \
        .withColumn("partition",
            sf.lit(datetime_result("today")))

    # Writing DF in trusted zone.
    write_trusted(DF_UNION_ITENS_GARNISH, 10, "partition", "order_items")


def trusted_order_status():

    PATH_ORDER = path_read_write("trusted", "order")
    DF_ORDER_STATUSES = spark.read \
        .parquet(PATH_ORDER) \

    # Creating temp view in memory for use in query below
    DF_ORDER_STATUSES.createOrReplaceTempView("order")
    RAW_STATUS.createOrReplaceTempView("status")

    DF_STATUSES_ORDER = spark.sql("""
    select  ord.order_id,
            st1.status_value status_1,
            st1.status_created_at status_created_at_1,
            st2.status_value status_2,
            st2.status_created_at status_created_at_2,
            st3.status_value status_3,
            st3.status_created_at status_created_at_3,
            st4.status_value status_4,
            st4.status_created_at status_created_at_4
    from order ord
    left join (
        select status_created_at, status_value, order_id
          from status
         where status_value = 'REGISTERED') st1 on st1.order_id = ord.order_id

    left join (
        select status_created_at, status_value, order_id
          from status
         where status_value = 'PLACED') st2 on st2.order_id = ord.order_id

    left join (
        select status_created_at, status_value, order_id
          from status
         where status_value = 'CONCLUDED') st3 on st3.order_id = ord.order_id

    left join (
        select status_created_at, status_value, order_id
          from status
         where status_value = 'CANCELLED') st4 on st4.order_id = ord.order_id
    """).dropDuplicates() \
    .withColumn("partition", sf.lit(datetime_result("today")))

    # Writing DF in trusted zone.
    write_trusted(DF_STATUSES_ORDER, 1, "partition", "order_statuses")

trusted_order()
trusted_order_items()
trusted_order_status()
