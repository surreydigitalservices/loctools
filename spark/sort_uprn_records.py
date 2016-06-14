## Spark Application - execute with spark-submit

import yaml
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


APP_NAME = "Sort UPRN Records"
SCHEMA_FILE = 'schema/addressbase_records.yml'

# RECORD_TYPES_TO_SORT = ['Organisation']
RECORD_TYPE_TO_SORT = '24'


class UPRNRecordSort:
    def __init__(self, sc, conf):
        self.sc = sc
        self.conf = conf
        self.schema_data = yaml.load(open(SCHEMA_FILE, 'r'))
        self.s3_root = conf.get('spark.custom.s3.root')
        self.sqlContext = SQLContext(sc)

    def get_schema(self, rec_id):
        fields = [StructField(field, StringType(), True) for field in self.schema_data[rec_id]['schema']]
        return StructType(fields)

    def run(self):
        ab_schema = self.get_schema(RECORD_TYPE_TO_SORT)
        record_type_name = self.schema_data[RECORD_TYPE_TO_SORT]['name']

        ab_file = self.s3_root + "/records/Addressbase_{0}.csv".format(record_type_name)
        ab_df = self.sqlContext.read.format('com.databricks.spark.csv').schema(ab_schema).load(ab_file)

        # Always sort by UPRN as that is the constant key across all property record types
        # ab_df_sorted = self.sqlContext.createDataFrame(ab_df.take(200000))
        # ab_df_sorted = ab_df_sorted.sort('UPRN')
        ab_df_sorted = ab_df.sort('UPRN')
        ab_df_sorted.show()

        ab_df_sorted.write.format('com.databricks.spark.csv').save(self.s3_root + "/sorted-records/Addressbase_{0}".format(record_type_name))



if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    merge = UPRNRecordSort(sc, conf)
    merge.run()
