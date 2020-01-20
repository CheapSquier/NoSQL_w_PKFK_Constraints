#!/usr/bin/python3
# coding: utf-8

# In[1]:


RunningDB = True
import aerospike
import sys
import json
import time
import random
import statistics
import pprint

print("Current Python version: ", sys.version_info[0],".",sys.version_info[1],".",sys.version_info[2])

_useAllTimes = False
_print_raw = False
# In[2]:


#Import the Table class used to build Table schema
from Private_Pkgs.Table import Table
from Private_Pkgs.DataRecordingUtils import drutils

def BuildTables():
    TableA = Table("test_DB_SEF", "TableA")
    TableA.AddCol("PKcol_TabA")
    for colNum in range(1,63+1):
        TableA.AddCol("col"+str(colNum))
    TableA.AddPK("PKcol_TabA")
    if TableA.VerifyFKrefsCompletePK():
        print(TableA.TableName, "has complete FK to PK references (or none exist in table)")

    TableB = Table("test_DB_SEF", "TableB")
    TableB.AddCol("PKcol_TabB")
    TableB.AddCol("FK_BtoA")
    for colNum in range(2,63+1):
        TableB.AddCol("col"+str(colNum))
    TableB.AddPK("PKcol_TabB")
    TableB.AddFK("FK_BtoA", "TableA", "PKcol_TabA")
    if TableB.VerifyFKrefsCompletePK():
        print(TableB.TableName, "has complete FK to PK references (or none exist in table)")

def InsertData():
    insertA_times = []
    insertB_times = []
    insertAll_times = {}
    TableA = Table._registry[0]
    TableB = Table._registry[1]
    print(TableA.TableName)
    print(TableB.TableName)
    for rowNum in range(0,int(NumberRows)):
        thisRow =[]
        thisRow.append(rowNum) #The PK
        for colNum in range(1,63+1):
            thisRow.append("datadata") # constant length
        t0 = time.time()
        TableA.Insert(thisRow)
        t1 = time.time()
        insertA_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
    for rowNum in range(0,int(NumberRows)):
        thisRow =[]
        thisRow.append(rowNum) #The PK
        #thisRow.append(int(NumberRows) - 1 - rowNum) #The FK # use this for a descending sequence
        thisRow.append(rowNum) #The FK (yes, same value as the PK) #ascending sequence
        for colNum in range(2,63+1):
            thisRow.append("datadata") # constant length
        t0 = time.time()
        TableB.Insert(thisRow)
        t1 = time.time()
        insertB_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
    insertAll_times["TableA"] = insertA_times
    insertAll_times["TableB"] = insertB_times
    return(insertAll_times)

def UpdateData():
    A_times = []
    B_times = []
    All_times = {}
    TableA = Table._registry[0]
    TableB = Table._registry[1]
    print(TableA.TableName)
    print(TableB.TableName)
    #Setup a random, no repeating sequence
    random.seed(42)
    row_seq = list(range(0, NumberRows))
    random.shuffle(row_seq)

    for idx in range(0, int(NumberRows)):
        thisRow =[]
        thisRow.append(row_seq[idx]) #The PK
        for colNum in range(1,63+1):
            thisRow.append("dataXXXX") # constant length
        t0 = time.time()
        TableA.Update(thisRow)
        #print("TableA update: ", thisRow)
        t1 = time.time()
        A_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
    for idx in range(0, int(NumberRows)):
        thisRow =[]
        thisRow.append(row_seq[idx]) #The PK
        thisRow.append(row_seq[int(NumberRows) - 1 - idx]) #The FK #descending
        #thisRow.append(row_seq[idx]) #The FK #ascending
        for colNum in range(2,63+1):
            thisRow.append("datadata") # constant length
        t0 = time.time()
        TableB.Update(thisRow)
        #print("TableB update: ", thisRow)
        t1 = time.time()
        B_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
    All_times["TableA"] = A_times
    All_times["TableB"] = B_times
    return(All_times)

def DeleteData():
    A_times = []
    B_times = []
    All_times = {}
    TableA = Table._registry[0]
    TableB = Table._registry[1]
    print(TableA.TableName)
    print(TableB.TableName)
    #Setup a random, no repeating sequence
    random.seed(42)
    row_seq = list(range(0, NumberRows))
    random.shuffle(row_seq)

    for idx in range(0, int(NumberRows/2)):
        A_Row = []
        B_Row = []
        A_Row.append(row_seq[idx]) #The PK
        B_Row.append(row_seq[idx]) #The PK for Table B
        t0 = time.time()
        TableB.Delete(B_Row)
        t1 = time.time()
        B_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
        t0 = time.time()
        TableA.Delete(A_Row)
        t1 = time.time()
        A_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
    All_times["TableA"] = A_times
    All_times["TableB"] = B_times
    return(All_times)

def report_times(insertAll_times, silent = False):
    BenchTimes = {}
    benchdata.rcrd_data(str("Statistics for Insert with {} rows, Verify Constraints set to: {}").format(NumberRows,Table.GetVerifyConstraints()))
    for dataKey in insertAll_times:
        insert_times = insertAll_times[dataKey]
        if _print_raw:
            print("Raw insert times for:", dataKey)
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(insert_times)
        BenchTimes[dataKey] = (NumberRows, Table.GetVerifyConstraints(), 
                         statistics.median(insert_times),
                         statistics.mean(insert_times),
                         statistics.stdev(insert_times))
        print("Statistics for: ", dataKey)
        benchdata.rcrd_data("\tMEDIAN"+str(statistics.median(insert_times))+" ms")
        benchdata.rcrd_data("\tMEAN  "+str(statistics.mean(insert_times))+" ms")
        benchdata.rcrd_data("\tSTDEV "+str(statistics.stdev(insert_times))+" ms")
    return BenchTimes

def report_statistics(withConBenchTimeAll, noConBenchTimeAll):
    for withConKey, noConKey in zip(withConBenchTimeAll, noConBenchTimeAll):
        print("Statistics for: ", withConKey)
        withConBenchTime = withConBenchTimeAll[withConKey]
        noConBenchTime = noConBenchTimeAll[noConKey]
        benchdata.rcrd_data()
        benchdata.rcrd_data("\tWith Constraints:\t\tWithout Constraints\t\tPercent Difference, {} rows".format(noConBenchTime[0]))
        benchdata.rcrd_data("MEDIAN:\t{} \t\t{} ms\t\t{}".format(withConBenchTime[2],noConBenchTime[2],
             (withConBenchTime[2]-noConBenchTime[2])/noConBenchTime[2]*100))
        benchdata.rcrd_data("MEAN:\t{} \t\t{} ms\t\t{}".format(withConBenchTime[3],noConBenchTime[3],
             (withConBenchTime[3]-noConBenchTime[2])/noConBenchTime[3]*100))
        benchdata.rcrd_data("STDEV:\t{} \t\t{} ms\t\t{}".format(withConBenchTime[4],noConBenchTime[4],
             (withConBenchTime[4]-noConBenchTime[4])/noConBenchTime[4]*100))
        benchdata.rcrd_data()
    return

Table.SetDebugMode(False)
Table.GetDebugMode()

# ***Open communication with the database***
# Specify the IP addresses and ports for the Aerospike cluster
config = {
  'hosts': [ ('18.221.189.247', 3000), ('18.224.137.105', 3000) ]
}
# Create a client and connect it to the cluster
print("RunningDB is", RunningDB)
if RunningDB:
    try:
      client = aerospike.client(config).connect()
    except:
      print("failed to connect to the cluster with", config['hosts'])
      sys.exit(1)
else:
    client = FakeAerospike(config) #.connect()
    print("WARNING: Running offline simulated version of databawe")

#Be sure to tell the Table class the name of the client it's talking to
Table.SetTableClient(client)

# ==============================================
#    Start of Main Program Loop
# ==============================================

if len(sys.argv) == 0:
    NumberRows = input("How many records to BenchMark?")
else:
    NumberRows = sys.argv[1]

NumberRows = int(NumberRows)
if NumberRows >= 1000:
    NumberRowsStr = str(int(NumberRows/1000))+"K"
else:
    NumberRowsStr = str(NumberRows)

#      ==============================================
#         Insert Section
#      ==============================================
#Initialize data reporting
benchdata =drutils("Schema1_Insert_expt_"+NumberRowsStr+"_","b")

Table.SetVerifyConstraints(True)
Table.UseFKTables(True)

# ***Build the tables***
BuildTables()

# ***Insert priming data, collect Insert times***
insertStartTime = time.time() #insertStartTime/EndTime only use to give an overall time for the insert loop
insertAll_times = InsertData()
insertEndTime = time.time()

# ### Calculate Time Statistics (Checking constraints during Insert)
withConstraintTimesAll = report_times(insertAll_times)

# ### Remove Benchmark Tables
Table.RemoveAllTables(client, True) #True to wait for confirmation

# ### Rebuild Tables and re-insert data, after disabling Constraints

Table.SetVerifyConstraints(False)
Table.UseFKTables(False)

BuildTables()
insertAll_times = InsertData()
noConstraintTimesAll = report_times(insertAll_times)

# ### Calculate Time Statistics (No constraint verification during Insert)    Close the data object
benchdata.rcrd_data("Total Insert Time: {} seconds, 2 tables".format(insertEndTime - insertStartTime))
report_statistics(withConstraintTimesAll, noConstraintTimesAll)
benchdata.close()
del benchdata

#      ==============================================
#         Update Section
#      ==============================================
benchdata =drutils("Schema1_Update_expt_"+NumberRowsStr+"_","b")

### We already have tables that do NOT use constraints or FK Tables ###
#Therefore, the first run of Update and Delete will be with ***No*** constraints
updateStartTime = time.time() #updateStartTime/EndTime only use to give an overall time for the update loop
updateAll_times = UpdateData()
updateEndTime = time.time()

noConstraintTimesAll = report_times(updateAll_times)

#***Remove and then Rebuild the Tables with constraints***
Table.RemoveAllTables(client, True) #True to wait for confirmation

Table.SetVerifyConstraints(True)
Table.UseFKTables(True)

BuildTables()
InsertData()

updateStartTime = time.time() #updateStartTime/EndTime only use to give an overall time for the update loop
updateAll_times = UpdateData()
updateEndTime = time.time()

withConstraintTimesAll = report_times(updateAll_times)

benchdata.rcrd_data("Total Update Time: {} seconds, 2 tables".format(updateEndTime - updateStartTime))
report_statistics(withConstraintTimesAll, noConstraintTimesAll)

benchdata.close()
del benchdata

#Remove all tables and rebuild them for the delete.
Table.RemoveAllTables(client, True) #True to wait for confirmation

#      ==============================================
#         Delete Section
#      ==============================================
benchdata =drutils("Schema1_Delete_expt_"+NumberRowsStr+"_","b")

Table.SetVerifyConstraints(True)
Table.UseFKTables(True)

BuildTables()
InsertData()

deleteStartTime = time.time() #deleteStartTime/EndTime only use to give an overall time for the delete loop
deleteAll_times = DeleteData()
deleteEndTime = time.time()

withConstraintTimesAll = report_times(deleteAll_times)

Table.RemoveAllTables(client, True) #True to wait for confirmation

Table.SetVerifyConstraints(False)
Table.UseFKTables(False)

BuildTables()
InsertData()

deleteStartTime = time.time() #deleteStartTime/EndTime only use to give an overall time for the delete loop
deleteAll_times = DeleteData()
deleteEndTime = time.time()

noConstraintTimesAll = report_times(deleteAll_times)

benchdata.rcrd_data("Total Delete Time: {} seconds, 2 tables".format(deleteEndTime - deleteStartTime))
report_statistics(withConstraintTimesAll, noConstraintTimesAll)

benchdata.close()
del benchdata

# Finally, remove the leftover tables
Table.RemoveAllTables(client, True) #True to wait for confirmation

client.close()
