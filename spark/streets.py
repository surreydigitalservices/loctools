## Spark Application - execute with spark-submit

## Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

## Module Constants
APP_NAME = "Merge Streets"
STREET_FILE = 'cache/AddressBasePremium_FULL_2016-04-27_001_r11.csv'
STREET_DESC_FILE = 'cache/AddressBasePremium_FULL_2016-04-27_002_r15.csv'

SCHEMA_FILE = 'schema/addressbase_records.yml'
## Closure Functions

## Main functionality

def get_schema(id):
    schema_data = yaml.load(open(SCHEMA_FILE, 'r'))
    fields = [StructField(field, StringType(), True) for field in config[id]['schema']]
    return StructType(fields)

def main(sc):
    sqlContext = SQLContext(sc)
    street_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(STREET_FILE)
    street_desc_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(STREET_DESC_FILE)
    streets_all_df = street_df.join(street_desc_df, 'USRN')
    streets_all_df.show()


if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
