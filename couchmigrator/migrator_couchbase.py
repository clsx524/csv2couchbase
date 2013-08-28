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
        self.client = Couchbase.connect(bucket=self.bucket, host=self.host, username=self.username, password=self.password)

    def write(self, record):
        def set (value, record, name): 
            if not name in value or value[name] == record[name]:
                value[name] = record[name]
            else:
                raise ValueError(name + " is different! " + "old value is " + value[name] + " new value is " + record[name])

        if self.type == "taskusage":
            try:
                self.client.add(str(record["id"]), record["Attributes"])
                print "id = %s" % record["id"]
            except KeyExistsError:
                raise KeyExistsError("Key has existed.")
                
        elif self.type == "taskevent":
            try:
                value = self.client[record["id"]].value   
            except NotFoundError:
                value = dict()
                value["count"] = 0
                self.client.add(record["id"], value)
            finally:
                value["count"] = value["count"] + 1
                arr = ["taskID", "jobID", "userName", "schedulingClass"]
                for var in arr:
                    set(value, record, var)

                value[value["count"]] = {"priority":record["priority"], "reqCPU":record["reqCPU"], "reqRAM":record["reqRAM"], "reqDisk":record["reqDisk"], "MachCont":record["MachCont"], "timeStamp":record["timeStamp"], "eventType":record["eventType"], "machineID":record["machineID"]}

                self.client.replace(record["id"], value)

        elif self.type == "jobevent":
            try:
                value = self.client[record["id"]].value  
            except NotFoundError:
                value = dict()
                value["count"] = 0
                self.client.add(record["id"], value)
            finally:
                value["count"] = value["count"] + 1
                arr = ["jobID", "userName", "jobName", "logicJobName"]
                for var in arr:
                    if var in value and value[var] != record[var]:
                        raise ValueError(var + " is different! " + "old value is " + value[var] + " new value is " + record[var])
                    set(value, record, var)

                value[value["count"]] = {"schedulingClass":record["schedulingClass"], "timeStamp":record["timeStamp"], "eventType":record["eventType"]}

                self.client.replace(record["id"], value)

        elif self.type == "machattrib":
            try:
                value = self.client[record["id"]].value  
            except NotFoundError:
                value = dict()
                value["count"] = 0
                self.client.add(record["id"], value)
            finally:
                value["count"] = value["count"] + 1
                #arr = ["machineID", "userName", "jobName", "logicJobName"]
                #for var in arr:
                #    if var in value and value[var] != record[var]:
                #        raise ValueError(var + " is different! " + "old value is " + value[var] + " new value is " + record[var])
                #    set(value, record, var)

                value[value["count"]] = {"timeStamp":record["timeStamp"], "attributeName":record["attributeName"], "attributeVal":record["attributeVal"], "attributeDel":record["attributeDel"]}

                self.client.replace(record["id"], value)

        elif self.type == "machineevent":
            # try:
            #     self.client.add(str(record["id"]), record["Attributes"])
            #     print "id = %s" % record["id"]
            # except KeyExistsError:
            #     raise KeyExistsError("Key has existed.")


            try:
                value = self.client[record["id"]].value  
            except NotFoundError:
                value = dict()
                value["count"] = 0
                self.client.add(record["id"], value)
            finally:
                value["count"] = value["count"] + 1
                arr = ["machineID"]
                for var in arr:
                    if var in value and value[var] != record[var]:
                        raise ValueError(var + " is different! " + "old value is " + value[var] + " new value is " + record[var])
                    set(value, record, var)

                value[value["count"]] = {"timeStamp":record["timeStamp"], "platformID":record["platformID"], "eventType":record["eventType"], "capCPU":record["capCPU"], "capMem":record["capMem"]}

                self.client.replace(record["id"], value)

        elif self.type == "taskconstraint":
            try:
                value = self.client[record["id"]].value   
            except NotFoundError:
                value = dict()
                value["count"] = 0
                self.client.add(record["id"], value)
            finally:
                value["count"] = value["count"] + 1
                arr = ["taskID", "jobID"]
                for var in arr:
                    set(value, record, var)

                value[value["count"]] = {"attributeName":record["attributeName"], "compareOperator":record["compareOperator"], "attributeVal":record["attributeVal"], "timeStamp":record["timeStamp"]}

                self.client.replace(record["id"], value)

        else:
            raise SyntaxError("Unknown Type of Table" + self.type)
