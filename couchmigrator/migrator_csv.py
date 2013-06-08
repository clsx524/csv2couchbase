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

        if self.type == "event" or self.type == "usage":
            self.readFunc = lambda type, row: self.readEventTable(row) if type == "event" else self.readUsageTable(row)
        else:
            raise SyntaxError("Unknown Type of Table" + self.type)
        
    def __iter__(self):
        return self

    def next(self):
        while True:
            row = self.reader.next()
            if row[1] == '' or self.type == 'usage':
                break

        return self.readFunc(self.type, row)

    def readEventTable(self, row):
        self.record["id"] = "%s-%s" % (row[2], row[3])
        self.record["Attributes"] = dict(jobID=row[2], taskID=row[3], machineID=row[4], timeStamp=row[0], userName=row[6], eventType=row[5], schedulingClass=row[7], priority=row[8], reqCPU=row[9], reqRAM=row[10], reqDisk=row[11], MachCont=row[12])
        return self.record

    def readUsageTable(self, row):
        self.record["id"] = self.number 
        self.number = self.number + 1 #"%s-%s" % (row[2], row[3])
        self.record["Attributes"] = dict(jobID=row[2], taskID=row[3], machineID=row[4], start=row[0], end=row[1], aCPUUsage=row[5], memUsage=row[6], memAssigned=row[7], UnmappedPageCacheUsage=row[8], pageCacheUsage=row[9], maxMemUsage=row[10], aDiskIOUsage=row[11], aDiskUsage=row[12], maxCPURate=row[13], maxDiskIO=row[14], CPI=row[15], MAI=row[16], samplingRate=row[17], aggregType=row[18])
        return self.record

class CSVWriter(couchmigrator.migrator.Writer):
    pass
