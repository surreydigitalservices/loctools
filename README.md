
## Setup

```
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

## Schema

`schema/addressbase_records.yml` contains a YAML-formatted definition of the Addressbase record types. Each `schema` block has a structure as defined by the JSON Table Schema specification. A `long` value for `type` is used where long integers are required (which is an extension to JSON Table Schema).


## Examples

```
luigi --local-scheduler --module tasks.addressbase CountAllRecordsTask

luigi --local-scheduler --module tasks.addressbase SplitAllRecordsTask
```

