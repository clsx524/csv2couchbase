csv2couchbase
=============


## Example commands:
	python couchbase-migrator.py -s csv:/home/eric/Copy/csv2couchbase/events.csv -d couchbase:clsx524:111111@localhost:8091/task_event -o -t event
	
	python couchbase-migrator.py -s csv:/home/eric/Copy/csv2couchbase/usage.csv -d couchbase:clsx524:111111@localhost:8091/task_usage -o -t usage

## Syntax: 

couchbase-migrator [options]

### Options:
 -h, --help

 -s <source>, --source=<source>
     Data source that will be imported from

 -d <destination>, --destination=<destination>
     Data destination that will be exported to

 -t <type>, --type=<type>
 	 Type, such as event or usage

### Sources:
 csv
     csv:<filename>

 json
     json:<filename>

 couchdb
     couchdb:example.com:5984/database


### Destinations:
 couchbase
     couchbase:username:password@example.com:8091/bucket

 json
     json:<filename>


### File formats:
 csv
"id","a","b","c"
"key1",1,"x","foo"
"key2",2,"y","bar"
"key3",3,"z","baz"

 json
{"id": "key1", "value": {"a": "1", "c": "foo", "b": "x"}}
{"id": "key2", "value": {"a": "2", "c": "bar", "b": "y"}}
{"id": "key3", "value": {"a": "3", "c": "baz", "b": "z"}}


### Requires:
 couchbase-python-client
  https://github.com/couchbase/couchbase-python-client


### Todo:
 Support either add or set for couchbase/membase/memcached destinations
 Support flags and expiry for couchbase/membase/memcached destinations
 Figure out how to deal with attachments from couchdb
 Speed up loading into couchbase, its painfully slow currently
