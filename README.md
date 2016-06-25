
## Setup

Prerequisites:

* Python 2.7
* virtualenv

Run these steps:

```
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

or run:

`make init`


### Tasks

**GetAllFTPFiles**

Downloads a set of files from an FTP site and stores them locally under `cache/from-os`.

**CountAllRecords**

Counts the record types in each file. Creates single manifest of results.

```
luigi --local-scheduler --module tasks.addressbase CountAllRecords
```

**MergeRecords**

**GroupByUPRN, GroupByUSRN**

Groups records with matching UPRNs and USRNs.

```
luigi --local-scheduler --module tasks.addressbase GroupByUPRN
luigi --local-scheduler --module tasks.addressbase GroupByUSRN
```


## Addressbase schema

`schema/addressbase_records.yml` contains a YAML-formatted definition of the Addressbase record types. Each `schema` block has a structure as defined by the [JSON Table Schema specification](http://dataprotocols.org/json-table-schema/). A `long` value is used for `type` where long integers are required (which is an extension to JSON Table Schema).

