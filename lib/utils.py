from pyspark import SparkConf
import configparser

URL = "jdbc:mysql://localhost:3306/"

properties = {
    "user": "root",
    "password": "Neoman#02",
    "driver": "com.mysql.cj.jdbc.Driver",
    "serverTimezone": "PST",
    "autoReconnect": "true",
    "useSSL": "false"
}


# Creating spark conf
def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("config/spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


# Reading tables mysql
def read_from_mysql_df(spark, tablename):
    return spark.read \
        .format("jdbc") \
        .options(url=URL, dbtable="employees" + "." + tablename, **properties) \
        .load()


# Writing the joined data to csv with headers
def write_to_target_csv(dataframe):
    dataframe.write \
        .format("csv") \
        .mode("overwrite") \
        .save("target/csv", header="true")
