sources=[{'type':'csv','class':'CSVReader','example':'csv:<filename>'}]
destinations=[]

import csv
import couchmigrator.migrator

class CSVReader(couchmigrator.migrator.Reader):
    def __init__(self, source, type):
        self.reader = csv.reader(open(source, 'rb'))
        self.record = dict()
        self.type = type
        self.number = 0

        if self.type == "taskevent":
            self.readFunc = lambda row: self.readTaskEventTable(row)
        elif self.type == "taskusage":
            self.readFunc = lambda row: self.readTaskUsageTable(row)
        elif self.type == "jobevent":
            self.readFunc = lambda row: self.readJobEventTable(row)
        elif self.type == "machattrib":
            self.readFunc = lambda row: self.readMachineAttributeTable(row)
        elif self.type == "machineevent":
            self.readFunc = lambda row: self.readMachineEventTable(row)
        elif self.type == "taskconstraint":
            self.readFunc = lambda row: self.readTaskConstraintTable(row)
        else:
            raise SyntaxError("Unknown Type of Table" + self.type)
        
    def __iter__(self):
        return self

    def next(self):
        while True:
            row = self.reader.next()
            if row[1] == '' or self.type == 'taskusage' or self.type == "machattrib" or self.type == "machineevent" or self.type == "taskconstraint":
                break

        return self.readFunc(row)

    def readTaskEventTable(self, row):
        self.record["id"] = "%s-%s" % (row[2], row[3])
        self.record["taskID"] = row[3]
        self.record["jobID"] = row[2]
        self.record["userName"] = row[6]
        self.record["schedulingClass"] = row[7]
        self.record["priority"] = row[8]
        self.record["timeStamp"] = row[0]
        self.record["reqCPU"] = row[9]
        self.record["reqRAM"] = row[10]
        self.record["reqDisk"] = row[11]
        self.record["eventType"] = row[5]
        self.record["machineID"] = row[4]
        self.record["MachCont"] = row[12]
        return self.record

    def readJobEventTable(self, row):
        self.record["id"] = row[2]
        self.record["timeStamp"] = row[0]
        self.record["jobID"] = row[2]
        self.record["eventType"] = row[3]
        self.record["userName"] = row[4]
        self.record["schedulingClass"] = row[5]
        self.record["jobName"] = row[6]
        self.record["logicJobName"] = row[7]

        return self.record

    def readTaskUsageTable(self, row):
        self.record["id"] = "%s-%s" % (row[2], row[3])
        self.record["Attributes"] = dict(jobID=row[2], taskID=row[3], machineID=row[4], start=row[0], end=row[1], aCPUUsage=row[5], memUsage=row[6], memAssigned=row[7], UnmappedPageCacheUsage=row[8], pageCacheUsage=row[9], maxMemUsage=row[10], aDiskIOUsage=row[11], aDiskUsage=row[12], maxCPURate=row[13], maxDiskIO=row[14], CPI=row[15], MAI=row[16], samplingRate=row[17], aggregType=row[18])
        return self.record

    def readMachineAttributeTable(self, row):
        self.record["id"] = row[1]
        self.record["machineID"] = row[1]
        self.record["timeStamp"] = row[0]
        self.record["attributeName"] = row[2]
        self.record["attributeVal"] = row[3]
        self.record["attributeDel"] = row[4]
        return self.record

    def readMachineEventTable(self, row):      
        self.record["id"] = row[1]
        self.record["machineID"] = row[1]
        self.record["timeStamp"] = row[0]
        self.record["eventType"] = row[2]
        self.record["platformID"] = row[3]
        self.record["capCPU"] = row[4]
        self.record["capMem"] = row[5]
        return self.record

    def readTaskConstraintTable(self, row):      
        self.record["id"] = "%s-%s" % (row[1], row[2])
        self.record["jobID"] = row[1]
        self.record["taskID"] = row[2]
        self.record["timeStamp"] = row[0]
        self.record["attributeName"] = row[3]
        self.record["compareOperator"] = row[4]
        self.record["attributeVal"] = row[5]

        return self.record

class CSVWriter(couchmigrator.migrator.Writer):
    pass
