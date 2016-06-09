

## Examples

luigi --local-scheduler --module tasks.addressbase AddressbaseDataTask

luigi --local-scheduler --module tasks.addressbase CountAllRecordsTask

luigi --local-scheduler --module tasks.addressbase SplitAllRecordsTask


time spark-submit --jars lib/spark-csv_2.11-1.4.0.jar,lib/commons-csv-1.4.jar spark/streets.py
time spark-submit --jars lib/spark-csv_2.11-1.4.0.jar,lib/commons-csv-1.4.jar spark/most_recent.py

