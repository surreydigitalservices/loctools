

## Examples

```
luigi --local-scheduler --module tasks.addressbase CountAllRecordsTask

luigi --local-scheduler --module tasks.addressbase SplitAllRecordsTask


spark-submit --jars lib/spark-csv_2.11-1.4.0.jar,lib/commons-csv-1.4.jar spark/streets.py
spark-submit --jars lib/spark-csv_2.11-1.4.0.jar,lib/commons-csv-1.4.jar spark/most_recent.py
```
