## Spark Application - execute with spark-submit
## This is designed to be run from a luigi SparkSubmitTask

import sys
import yaml
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


# APP_NAME = "Sort UPRN Records"
SCHEMA_FILE = 'schema/addressbase_records.yml'

FIELD_TYPES_MAP = {
    'string': StringType,
    'integer': IntegerType,
    'long': LongType,
    'number': DoubleType,
    'date': DateType,
    'time': TimestampType,
}


class UPRNRecordSort:
    def __init__(self, sc, path_root, record_types):
        self.sc = sc
        self.schema_data = yaml.load(open(SCHEMA_FILE, 'r'))
        self.path_root = path_root
        self.record_types = record_types
        self.sqlContext = SQLContext(sc)

    def get_schema(self, rec_id):
        fields = []
        for field in self.schema_data[rec_id]['schema']['fields']:
            print field['name']
            fields.append(StructField(field['name'], FIELD_TYPES_MAP[field['type']](), True))

        return StructType(fields)

    def run(self):
        for record_type in self.record_types:
            ab_schema = self.get_schema(record_type)
            record_type_name = self.schema_data[record_type]['name']

            ab_file = self.path_root + "/records/Addressbase_{0}.csv".format(record_type_name)
            ab_df = self.sqlContext.read.format('com.databricks.spark.csv').schema(ab_schema).load(ab_file)

            # Always sort by UPRN as that is the constant key across all property record types
            ab_df_sorted = ab_df.sort('UPRN')
            ab_df_sorted.show()

            out_path = self.path_root + "/sorted_records/Addressbase_{0}".format(record_type_name)
            ab_df_sorted.write.format('com.databricks.spark.csv').option('nullValue', '').save(out_path)



if __name__ == "__main__":
    sc   = SparkContext()
    path_root = sys.argv[1]
    # Assume the record types is a comma-separated list of type numbers
    record_types = sys.argv[2].split(',')
    record_types = map(str.strip, record_types)
    merge = UPRNRecordSort(sc, path_root, record_types)
    merge.run()

