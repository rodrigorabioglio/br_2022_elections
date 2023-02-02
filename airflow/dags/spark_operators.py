from pyspark.sql import (
    SparkSession,
    functions as F,
)

from const import SCHEMA, COLS, JAR_DIR, DATA_DIR, execution_dict

def prepare_data_and_write_to_bigquery(bucket_name, state):

    spark = SparkSession\
        .builder\
        .appName("IngestToBigQuery")\
        .master('yarn')\
        .getOrCreate()

    spark.conf.set('temporaryGcsBucket', bucket_name)

    # read data with schema
    df = (
        spark
        .read
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "latin1")
        .schema(SCHEMA)
        .csv(f"gs://{bucket_name}/{state}/*")
    )

    # prepare_data
    prepared = df\
        .withColumn('TS_GERACAO',F.to_timestamp(F.concat(df.HH_GERACAO, F.lit(' '), df.DT_GERACAO),'dd/MM/yyy HH:mm:ss'))\
        .select(COLS)\

    # rename columns
    prepared=prepared.toDF(*[c.lower() for c in prepared.columns])

    prepared.write.format('bigquery') \
    .option('table', 'eleicoes.teste') \
    .save()
