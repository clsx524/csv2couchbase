__version__ = '1.0.0'
__all__ = [
    'reader', 'writer',
]

from migrator_csv import CSVReader, CSVWriter
from migrator_json import JSONReader, JSONWriter
from migrator_couchbase import CouchbaseReader, CouchbaseWriter

sources = []
destinations = []

sources.extend(migrator_couchbase.sources)
sources.extend(migrator_csv.sources)
sources.extend(migrator_json.sources)

destinations.extend(migrator_couchbase.destinations)
destinations.extend(migrator_csv.destinations)
destinations.extend(migrator_json.destinations)

def reader(loc, type):
    kind, fp = loc.split(':', 1)
    if kind.lower() == 'csv':
        return CSVReader(fp, type)
    elif kind.lower() == 'json':
        return JSONReader(fp)
    elif kind.lower() == 'couchbase':
        return CouchbaseReader(fp, '')

def writer(loc, type):
    kind, fp = loc.split(':', 1)
    if kind.lower() == 'csv':
        return CSVWriter(fp)
    elif kind.lower() == 'json':
        return JSONWriter(fp)
    elif kind.lower() == 'couchbase':
        return CouchbaseWriter(fp, type)
