from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import sys
from pyspark.sql.types import *

# loggers configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../../log/debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)


def session_object():
    # creating session object
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    logging.info("spark session created")
    return spark


def create_df(spark):
    # creating dataframe
    logging.info("schema and data for product table")
    product_schema = StructType([StructField("Product Name", StringType(), True),
                                 StructField("Issue Date", StringType(), True),
                                 StructField("Price", IntegerType(), True),
                                 StructField("Brand", StringType(), True),
                                 StructField("Country", StringType(), True),
                                 StructField("Product number", IntegerType(), True)
                                 ])
    product_data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1),
                    ("Refrigerator", "1648770999000", 35000, " LG", "null", 2),
                    ("Air Cooler", "1648770948000", 45000, " Voltas", "null", 3)]

    product_df = spark.createDataFrame(data=product_data, schema=product_schema)
    logging.info("Product dataframe is : ")
    product_df.show()
    return product_df


def date_time(product_df):
    product_df.printSchema()
    product_date = product_df.withColumn("timestamp_date", from_unixtime((col("Issue Date") / 1000)))
    logging.info("Updated timestamp date : ")
    return product_date


def date_type(product_date):
    date_df = product_date.withColumn('date_type', col('timestamp_date').cast('date'))
    logging.info("Sorting date from timestamp date : ")
    return date_df


def space_remove(product_df):
    space_removed_df = product_df.withColumn('Brand_trim', trim(col('Brand')))
    logging.info("Removing the spaces in Brand column : ")
    return space_removed_df


def null_empty(product_df):
    empty_df = product_df.withColumn('Country', when(product_df.Country == "null", "")
                                     .otherwise(product_df.Country))
    logging.info("Converting null to empty : ")
    return empty_df


def transaction_table(spark):
    transact_schema = StructType([StructField("SourceId", IntegerType(), True),
                                  StructField("TransactionNumber", IntegerType(), True),
                                  StructField("Language", StringType(), True),
                                  StructField("ModelNumber", IntegerType(), True),
                                  StructField("StartTime", StringType(), True),
                                  StructField("Product Number", IntegerType(), True)
                                  ])
    transact_data = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1),
                     (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", 2),
                     (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", 3)]

    transact_df = spark.createDataFrame(data=transact_data, schema=transact_schema)
    return transact_df


def camel_snake(transact_df):
    new_snack_col = transact_df.withColumnRenamed("SourceId", "sourceid") \
        .withColumnRenamed("TransactionNumber", "transactionnumber") \
        .withColumnRenamed("Language", "language") \
        .withColumnRenamed("ModelNumber", "modelnumber") \
        .withColumnRenamed("StartTime", "starttime") \
        .withColumnRenamed("Product Number", "product number")
    return new_snack_col


def start_time_ms(transact_df):
    ms_df = transact_df.withColumn('start_time_ms', unix_timestamp("starttime", "yyyy-MM-dd HH:mm:ss.SSS"))
    return ms_df


def combine_df(transact_df,product_df, col1, col2, join_type):
    join_df = product_df.join(transact_df, product_df[col1] == transact_df[col2], join_type).drop("Product Number")
    return join_df


def country_en(join_df):
    filter_df = join_df.filter(col("Language") == "EN")
    country_df = filter_df.select("Country")
    return country_df
