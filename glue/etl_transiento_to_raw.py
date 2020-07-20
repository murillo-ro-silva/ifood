import sys
import logging
import datetime as dt

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
import pyspark.sql.functions as sf

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


def args_datasets_read_types(sys_args):
    """ param sys_args: Receive all atributos from Glue UI param.
        All datasets with "--n_" prefixes are being treating
        with datasets and read types.
        e.g:
            --n_consumer / csv.
            --n_order / json
            --n_....
        return dict with datasets and read types.
    """
    datasets_read_types = []
    for liu in enumerate(sys_args):
        if '--n_' in liu[1]:
            dataset = liu[1][4:]
            read_type = sys_args[liu[0]+1]
            datasets_read_types.append([dataset, read_type])
    return datasets_read_types


def datetime_result(date_type):
    """ Recover brazilian time.
    """
    if date_type == 'today':
        date_now = (
            dt.datetime.utcnow() - dt.timedelta(hours=3)).strftime('%Y-%m-%d')
        return str(date_now)
    if date_type == 'today_timestamp':
        date_now = (
            dt.datetime.utcnow() - dt.timedelta(hours=3)) \
            .strftime('%Y-%m-%d %H:%M:%S')
        return str(date_now)
    else:
        date_yesternoon = (
            dt.datetime.utcnow() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
        return str(date_yesternoon)


def path_read_write(zone, dataset_name, read_type):
    """
        refurn absolute bucket path.
    """
    BASE_PATH_SOURCE = 's3://ifood-data-architect-test-source'

    if zone == 'source':
        PATH = BASE_PATH_SOURCE + '/' + dataset_name + '.' + read_type + '.gz'
    else:
        PATH = BASE_PATH_SOURCE + '-murillo/' + zone + '/' + dataset_name

    return PATH


def read_dataset(zone, dataset_name, read_type):
    """
    e.g:
        Read json and csv from source repo, creating
        partition column setting now date.
        return spark dataframe
    """
    PARTITION_DATE = datetime_result('today')
    PATH = path_read_write(zone, dataset_name, read_type)

    READ = spark.read \
        .load(PATH, format=read_type, header=True, infer_schema=True) \
        .withColumn('partition_column', sf.lit(PARTITION_DATE))

    return READ


def write_dataset(zone, dataset_name, df):
    """ param zone (transient/raw/refined/trusted)
        param dataset_name
        param df (spark dataframe)
        e.g:
            Write some data in .parquet extension,
            overwriting existing data inside the partition.
        return spark dataframe
    """
    PATH = path_read_write(zone, dataset_name, '.parquet')
    df.write \
      .mode('overwrite') \
      .partitionBy('partition_column') \
      .parquet(PATH)


for dataset_name, type_read in args_datasets_read_types(sys.argv):
    """ Iterate from all datasets and read_types configurable.
    """

    logger.info('Load Transient:' + datetime_result('today_timestamp') + ' - Dataset: ' + dataset_name + ' - type_read: ' + type_read)
    df = read_dataset('source', dataset_name, type_read)

    logger.info('Write Raw: ' + datetime_result('today_timestamp') + ' - Dataset: ' + dataset_name + ' - type_read: ' + type_read)
    write_dataset('raw', dataset_name, df)
