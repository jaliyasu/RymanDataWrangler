
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as fun
from pyspark.sql.types import *


spark = SparkSession.builder.master("local[1]") \
    .appName("assignment1.part2.com") \
    .getOrCreate()

class CsvDataWrangler:

    def __init__(self, filepath: str):
        self.__path = filepath

    def read_file(self) -> DataFrame:
        raw_df = spark.read.csv(self.__path,
                                header='true',
                                sep=',')

        raw_df.show()

if __name__ == '__main__':
    # Read dataset in to data frame
    raw_df_for_wrangling = CsvDataWrangler("../data_repo/Log.csv").read_file()