from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as fun
from pyspark.sql.types import *
import os

spark = SparkSession.builder \
    .appName("MySQL Connector with PySpark") \
    .config("spark.jars", "../jdbc_driver/mysql-connector-j-9.2.0.jar") \
    .getOrCreate()

class MysqlDataWrangler:

    def __init__(self):
        self.mysql_url = "jdbc:mysql://localhost:3306/adventureworks"  # hostname, port, and database name
        self.mysql_properties = {
            "user": "root",  # Change to your MySQL username
            "password": os.getenv("MYSQL_DB_PASSWORD"),  # MySQL password
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    def read_data(self):
        df = spark.read.jdbc(url=self.mysql_url, table="address", properties=self.mysql_properties)
        df.show()


    def join_tables(self):
        df = spark.read.jdbc(url=self.mysql_url, table="address", properties=self.mysql_properties)

if __name__ == '__main__':
    # Read dataset in to data frame
    raw_df_for_wrangling = MysqlDataWrangler().read_data()
