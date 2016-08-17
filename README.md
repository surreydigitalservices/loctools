# loctools

A [Luigi](https://github.com/spotify/luigi) project that defines data processing steps on Addressbase data drops.


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


## Configuration

Configuration settings are defined by:

* `luigi.cfg` file
* Via the command line, e.g. `--password abc123`

See the [Luigi docs](https://luigi.readthedocs.io/en/stable/command_line.html) for more info.

## Tasks

**GetAllFTPFiles**

Downloads a set of files from an FTP site and stores them locally under `cache/from-os`.

**CountAllRecords**

Counts the record types in each file. Creates a single manifest of results.

```
luigi --local-scheduler --module tasks.addressbase CountAllRecords
```

**MergeAllRecords**

Merges all records with the same type into single files.

**SortRecords**

Sorts the record data across multiple types into a series of files with related records (same UPRN/USRN) co-located.

**GroupByUPRN, GroupByUSRN**

Groups records with matching UPRNs and USRNs.

```
luigi --local-scheduler --module tasks.addressbase GroupByUPRN
luigi --local-scheduler --module tasks.addressbase GroupByUSRN
```


## Addressbase schema

`schema/addressbase_records.yml` contains a YAML-formatted definition of the Addressbase record types. Each `schema` block has a structure as defined by the [JSON Table Schema specification](http://dataprotocols.org/json-table-schema/). A `long` value is used for `type` where long integers are required (which is an extension to JSON Table Schema).

