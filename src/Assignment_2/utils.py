from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def spark_session():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark


def create_df(spark):
    user_schema = StructType([StructField("name", StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)])),
                              StructField("dob", StringType(), True),
                              StructField("gender", StringType(), True),
                              StructField("salary", IntegerType(), True)
                              ])
    user_data = [(("James", "", "Smith"), "03011998", "M", 3000),
                 (("Michale", "Rose", ""), "10111998", "M", 20000),
                 (("Robert", "", "Williams"), "02012000", "M", 3000),
                 (("Maria", "Anne", "Jones"), "03011998", "F", 11000),
                 (("Jen", "Mary", "Brown"), "04101998", "F", 10000)
                 ]

    user_df = spark.createDataFrame(data=user_data, schema=user_schema)
    return user_df


def select_df(user_df):
    select_col_df = user_df.select("name.firstname", "name.lastname", "salary")
    return select_col_df


def add_column(user_df):
    user_new_df = user_df.withColumn("country", lit("India")) \
        .withColumn("department", lit("D001")) \
        .withColumn("age", lit(20))
    return user_new_df


def salary_change(user_df):
    salary_df = user_df.withColumn("salary", when(col("salary") == 3000, col("salary") + 2000)
                                   .otherwise(col("salary")))
    return salary_df


def change_type(user_df):
    changed_type_df = user_df.withColumn("dob", col("dob").cast("string")) \
        .withColumn("salary", col("salary").cast("string"))
    return changed_type_df


def Salary_bonus(user_df):
    bonus_salary = user_df.withColumn("Salary_bonus", when(col("salary") == 3000, col("salary") + 2000)
                                      .otherwise(col("salary")))
    return bonus_salary


def nested_col(user_df):
    nested_col_df = user_df.select(col("name.firstname").alias("firstposition"),
                                   col("name.middlename").alias("secondposition"),
                                   col("name.lastname").alias("lastposition")
                                   )
    return nested_col_df


def name_filter(user_df):
    max_salary = user_df.agg(max("salary")).first()[0]
    name_filter_df = user_df.filter(user_df.salary == max_salary).select("name").first()
    return name_filter_df


def drop_column(user_new_df):
    columns_drop = ["department", "age"]
    drop_df = user_new_df.drop(*columns_drop)
    return drop_df


def distinct_column(user_df):
    distinct_df = user_df.select("dob", "salary").distinct()
    return distinct_df
