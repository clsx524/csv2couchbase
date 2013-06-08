# created by Eric
# All rights reserved.

sources=[]
destinations=[{'type':'couchbase','class':'CouchbaseWriter','example':'couchbase:username:password@example.com:8091/bucket'}]

import re
import json

from couchbase import Couchbase
from couchbase.exceptions import TimeoutError, NotFoundError, KeyExistsError

import couchmigrator.migrator

class CouchbaseReader(couchmigrator.migrator.Reader):
    def __init__(self, source):
        # username:password@example.com:8091/bucket
        m = re.match('^([^:]+):([^@]+)@([^:]+):([^/]+)/(.+)$', source)
        self.username = m.group(1)
        self.password = m.group(2)
        self.host = m.group(3)
        self.port = m.group(4)
        self.bucket = m.group(5)

        self.bucket_port = 11211
        self.bucket_password = ''

    def __iter__(self):
       return self

    def next(self):
       data = self.reader.next()
       if data:
           record = {'id':data['id']}
           record['value'] = dict((k,v) for (k,v) in json_data['value'].iteritems() if not k.startswith('_'))
           return record
       else:
           raise StopIteration()
       raise StopIteration()


class CouchbaseWriter(couchmigrator.migrator.Writer):
    def __init__(self, destination, type):
        # username:password@example.com:8091/bucket        
        m = re.match('^([^:]+):([^@]+)@([^:]+):([^/]+)/(.+)$', destination)
        self.username = m.group(1)
        self.password = m.group(2)
        self.host = m.group(3)
        self.port = m.group(4)
        self.bucket = m.group(5)

        self.type = type

        #self.bucket_port = 11211
        #self.bucket_password = ''

        #self.verbose = True

        # todo: use server username/password to query the bucket password/port if needed
        #self.server = "http://%s:%s/pools/default" % (self.host, self.port)
        self.client = Couchbase.connect(bucket=self.bucket, host=self.host, username='clsx524', password='111111')

    def write(self, record):
        if self.type == "usage":
            try:
                self.client.add(str(record["id"]), record["Attributes"])
                print "id = %s" % record["id"]
            except KeyExistsError:
                raise KeyExistsError("Key has existed.")
                
        elif self.type == "event":
            try:
                value = self.client[record["id"]].value   
            except NotFoundError:
                value = dict()
                value["count"] = 0
                self.client.add(record["id"], value)
            finally:
                value["count"] = value["count"] + 1
                value[value["count"]] = record["Attributes"]
                self.client.replace(record["id"], value)
                print "id = %s" % (record["id"])

        else:
            raise SyntaxError("Unknown Type of Table" + self.type)

    #def close(self):
    #    self.client.done()
