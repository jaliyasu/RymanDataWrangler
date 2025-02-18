from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as fun
from pyspark.sql.types import *
import os

spark = SparkSession.builder \
    .appName("MSSQL Connector with PySpark") \
    .config("spark.jars", "../jdbc_driver/mssql-jdbc-12.8.1.jre8.jar") \
    .getOrCreate()


class MsSqlDataWrangler:

    def __init__(self):
        self.mssqlsql_url = "jdbc:sqlserver://msselserver18022025.database.windows.net:1433;database=mydb_180220205"  # hostname, port, and database name
        self.mssql_properties = {
            "user": "userroot@msselserver18022025",  # MSSQL username
            "password": os.getenv("MSSQL_DB_PASSWORD"),  # MSQL password
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

    def read_data(self):
        df_customer = spark.read.jdbc(url=self.mssqlsql_url, table="SalesLT.Customer", properties=self.mssql_properties)
        df_sales_order_header = spark.read.jdbc(url=self.mssqlsql_url, table="[SalesLT].[SalesOrderHeader]",
                                                properties=self.mssql_properties)

        df_sales_order_header.join(df_customer,df_customer.CustomerID==df_sales_order_header.CustomerID,'inner')\
            .drop(df_customer.CustomerID).createOrReplaceTempView("customer_performance")

        spark.sql("SELECT customerID,TotalDue FROM customer_performance").show()

    def read_password(self):
        print()


if __name__ == '__main__':
    # Read dataset in to data frame
    raw_df_for_wrangling = MsSqlDataWrangler().read_data()
