from pyspark.sql.functions import col, broadcast
from lib.utils import get_spark_app_config, write_to_target_csv
from lib.utils import read_from_mysql_df
from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting the pyspark application")

    # Reading employee data from mysql
    employee_df = read_from_mysql_df(spark, "employees")

    # Reading common table column from mysql
    dept_emp_df = read_from_mysql_df(spark, "dept_emp")

    # Renaming the emp_no column to avoid ambiguity
    empno_renamed = dept_emp_df.withColumnRenamed("emp_no", "dept_emp_no")
    # Join Expression
    join_expr = employee_df.emp_no == empno_renamed.dept_emp_no

    # Inner Join
    empno_renamed.join(employee_df, join_expr, "inner") \
        .select(col("emp_no"), col("dept_no"), col("first_name"), col("last_name"), col("gender"), col("hire_Date")) \
        .show()

    # Outer Join
    department_df = read_from_mysql_df(spark, "departments")

    # BroadCast join on smaller dataset
    renamed_df = employee_df.join(broadcast(empno_renamed), join_expr, "inner") \
        .select(col("*")) \
        .sort(col("hire_date"))

    # input("press any key to continue")
    join_expr_dept = department_df.dept_no == empno_renamed.dept_no

    # BroadCast join on smaller dataset
    results_to_file = renamed_df.join(broadcast(department_df), join_expr_dept, "left") \
        .drop(department_df.dept_no) \
        .select(col("*"))
        #.show(False)
    # .show(renamed_df.count(), False) \

    write_to_target_csv(results_to_file)

    input("press any key to continue")
    logger.info("Completing the pyspark application")
