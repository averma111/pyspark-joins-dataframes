from pyspark.sql.functions import col, broadcast
from lib.utils import get_spark_app_config, write_to_target_csv
from lib.utils import read_from_mysql_df
from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting the pyspark application")

    # Reading employee data from mysql
    employee_df = read_from_mysql_df(spark, "employees")

    # Reading common table column from mysql
    dept_emp_df = read_from_mysql_df(spark, "dept_emp")

    # Renaming the emp_no column to avoid ambiguity
    empno_renamed = dept_emp_df.withColumnRenamed("emp_no", "dept_emp_no")

    # Creating database in derby database
    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("use MY_DB")

    # Creating employee bucket
    employee_df.coalesce(1) \
        .write \
        .bucketBy(4, "emp_no") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.employee_bucket")

    # Creating Dept_Emp_no bucket
    empno_renamed.coalesce(1) \
        .write \
        .bucketBy(4, "dept_emp_no") \
        .mode("overwrite") \
        .saveAsTable("MY_DB.dept_empno_bucket")

    df1 = spark.read.table("MY_DB.employee_bucket")
    df2 = spark.read.table("MY_DB.dept_empno_bucket")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    # Join Expression
    join_expr = df1.emp_no == df2.dept_emp_no

    # Inner Join
    joined_df = df1.join(df2, join_expr, "inner") \
        .select(col("emp_no"), col("dept_no"), col("first_name"), col("last_name"), col("gender"), col("hire_Date")) \
        .show()

    input("Press any key to continue")
    logger.info("Completing the pyspark application")
