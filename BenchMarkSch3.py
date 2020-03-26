#!/usr/bin/python3
# coding: utf-8

# In[1]:


RunningDB = True
#import aerospike
import mysql.connector
from mysql.connector import errorcode
import sys
import json
import time
import random
import statistics
import pprint
import math

print("Current Python version: ", sys.version_info[0],".",sys.version_info[1],".",sys.version_info[2])

_useAllTimes = False
_print_raw = False
_DB_mode = sys.argv[1]
if _DB_mode != "AS" and _DB_mode != "SQL":
    print("ERROR: MUST SPECIFY AS OR SQL MODE AS 1ST PARAMETER")
    exit()
# In[2]:

#Import the Table class used to build Table schema
#from Private_Pkgs.AS_Table import Table
from Private_Pkgs.SQL_Table import Table
from Private_Pkgs.DataRecordingUtils import drutils

def BuildTables():
    TableA = Table("test_DB_SEF", "Table3A")
    TableA.AddCol("PKcol_TabA")
    TableA.AddCol("DataColA")
    TableA.AddPK("PKcol_TabA")
    if TableA.VerifyFKrefsCompletePK(): # In SQL mode, this will create the table
        if _DB_mode == "AS":
            print(TableA.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableA.TableName, "has been created in the SQL database")

    TableB = Table("test_DB_SEF", "Table3B")
    TableB.AddCol("PKcol_TabB")
    TableB.AddCol("DataColB")
    TableB.AddPK("PKcol_TabB")
    if TableB.VerifyFKrefsCompletePK():
        if _DB_mode == "AS":
            print(TableB.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableB.TableName, "has been created in the SQL database")

    TableC = Table("test_DB_SEF", "Table3C")
    TableC.AddCol("FK_CtoA")
    TableC.AddCol("FK_CtoB")
    TableC.AddCol("PKcol_TabC")
    TableC.AddCol("DataColC")
    TableC.AddPK("FK_CtoA")
    TableC.AddPK("FK_CtoB")
    TableC.AddPK("PKcol_TabC")
    TableC.AddFK("FK_CtoA", "Table3A", "PKcol_TabA")
    TableC.AddFK("FK_CtoB", "Table3B", "PKcol_TabB")
    if TableC.VerifyFKrefsCompletePK():
        if _DB_mode == "AS":
            print(TableC.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableC.TableName, "has been created in the SQL database")

def InsertData(NumberHashesPerFK):
    TableC_times = []
    insertAll_times = {}
    TableA = Table._registry[0]
    TableB = Table._registry[1]
    TableC = Table._registry[2]
    for rowNum in range(0,int(NumberRows)):
        thisRow = [rowNum,"datadata"]
        TableA.Insert(thisRow)
        thisRow = [rowNum,"datadata"]
        TableB.Insert(thisRow)

    for rowNum in range(0,int(NumberRows)):
        FKindex = math.floor(rowNum/NumberHashesPerFK)
        thisRow =  [FKindex, FKindex, rowNum, "datadata"] #The composite PK. Both rowNums are FKs
        t0 = time.time()
        TableC.Insert(thisRow)
        t1 = time.time()
        TableC_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
    insertAll_times["TableC"] = TableC_times
    return(insertAll_times)

def UpdateData(NumberHashesPerFK):
    TableC_times = []
    All_times = {}
    #TableA = Table._registry[0] # Not updating these tables so these lines aren't needed
    #TableB = Table._registry[1]
    TableC = Table._registry[2]
    #Setup a random, no repeating sequence
    random.seed(42)
    row_seq = list(range(0, NumberRows))
    random.shuffle(row_seq)
    for idx in range(0, int(NumberRows)):
        FKindex = math.floor(row_seq[idx]/NumberHashesPerFK)
        thisRow =[]
        thisRow.append(FKindex) #1st element of PK
        thisRow.append(FKindex) #2nd element of PK
        thisRow.append(row_seq[idx]) # 3rd element of PK that will make the PK unique
        thisRow.append("dataXXXX")
        t0 = time.time()
        TableC.Update(thisRow)
        #print("TableA update: ", thisRow)
        t1 = time.time()
        TableC_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
    All_times["TableC"] = TableC_times
    return(All_times)

def DeleteData(NumberHashesPerFK):
    TableA_times = []
    TableB_times = []
    TableC_times = []
    All_times = {}
    TableA = Table._registry[0]
    TableB = Table._registry[1]
    TableC = Table._registry[2]
    #Setup a random, no repeating sequence
    random.seed(42)
    row_seq = list(range(0, NumberRows))
    random.shuffle(row_seq)
    
    for idx in range(0, int(NumberRows/2)):
        FKindex = math.floor(row_seq[idx]/NumberHashesPerFK)
        t0 = time.time()
        TableC.Delete([row_seq[idx], row_seq[idx], FKindex])
        t1 = time.time()
        TableC_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
        t0 = time.time()
        TableA.Delete([row_seq[idx]])
        t1 = time.time()
        TableA_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
        t0 = time.time()
        TableB.Delete([row_seq[idx]])
        t1 = time.time()
        TableB_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
    All_times["TableC"] = TableC_times
    All_times["TableA"] = TableA_times
    All_times["TableB"] = TableB_times
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
        benchdata.rcrd_data("Statistics for: "+dataKey)
        benchdata.rcrd_data("\tMEDIAN"+str(statistics.median(insert_times))+" ms")
        benchdata.rcrd_data("\tMEAN  "+str(statistics.mean(insert_times))+" ms")
        benchdata.rcrd_data("\tSTDEV "+str(statistics.stdev(insert_times))+" ms")
    return BenchTimes

def report_statistics(withConBenchTimeAll, noConBenchTimeAll):
    for withConKey, noConKey in zip(withConBenchTimeAll, noConBenchTimeAll): # FIXME This function needes to handle noCon times that are empty (for SQL)
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
if sys.argv[1] == "AS":
    # Specify the IP addresses and ports for the Aerospike cluster
    config = {
    'hosts': [ ('18.221.189.247', 3000), ('18.224.137.105', 3000) ]
    }
    # Create a client and connect it to the cluster
    print("RunningDB is", RunningDB)
    try:
        client = aerospike.client(config).connect()
        #Be sure to tell the Table class the name of the client it's talking to
        Table.SetTableClient(client)
    except:
        print("failed to connect to the cluster with", config['hosts'])
        sys.exit(1)
if sys.argv[1] == "SQL":
    client = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    database="test_db_sql"
    #host="18.191.176.248",
    #user="demouser",
    #passwd="DrBajaj2*",
    )
    sqlCursor = client.cursor(buffered=True)
    Table.SetTableClient(client, sqlCursor)
if sys.argv[1] != "SQL" and sys.argv[1] != "AS":
    print("EXITING: Must specify AS or SQL database to run with")
    sys.exit(1)


# ==============================================
#    Start of Main Program Loop
# ==============================================

if len(sys.argv) == 1: # Only specified the DB type, so input params
    NumberRows = input("How many records to BenchMark?")
    FKreptFactor = int(input("What FK repetition factor to use?"))
else:
    NumberRows = int(sys.argv[2])
    FKreptFactor = int(sys.argv[3])

NumberRows = int(NumberRows)
if NumberRows >= 1000:
    NumberRowsStr = str(int(NumberRows/1000))+"K"
else:
    NumberRowsStr = str(NumberRows)

#      ==============================================
#         Insert Section
#      ==============================================
#Initialize data reporting
benchdata =drutils("Schema3_Insert_expt_"+sys.argv[1]+"_"+NumberRowsStr+"_","b")

Table.SetVerifyConstraints(True)
Table.UseFKTables(True)

# ***Build the tables***
BuildTables()

# ***Insert priming data, collect Insert times***
insertStartTime = time.time() #insertStartTime/EndTime only use to give an overall time for the insert loop
insertAll_times = InsertData(FKreptFactor)
insertEndTime = time.time()

# ### Calculate Time Statistics (Checking constraints during Insert)
withConstraintTimesAll = report_times(insertAll_times)
noConstraintTimesAll = []
if sys.argv[1] == "AS":
    # ### Remove Benchmark Tables
    Table.RemoveAllTables(client, True) #True to wait for confirmation

    # ### Rebuild Tables and re-insert data, after disabling Constraints

    Table.SetVerifyConstraints(False)
    Table.UseFKTables(False)

    BuildTables()
    insertAll_times = InsertData(FKreptFactor)
    noConstraintTimesAll = report_times(insertAll_times)

# ### Calculate Time Statistics (No constraint verification during Insert)    Close the data object
benchdata.rcrd_data("Total Insert Time: {} seconds, 2 tables".format(insertEndTime - insertStartTime))
report_statistics(withConstraintTimesAll, noConstraintTimesAll) # report_statistics will handle empty noConstraint lists
benchdata.close()
del benchdata


#      ==============================================
#         Update Section
#      ==============================================
benchdata =drutils("Schema3_Update_expt_"+sys.argv[1]+"_"+NumberRowsStr+"_","b")

noConstraintTimesAll = []
if sys.argv[1] == "AS":
    ### We already have tables that do NOT use constraints or FK Tables ###
    #Therefore, the first run of Update and Delete will be with ***No*** constraints
    updateStartTime = time.time() #updateStartTime/EndTime only use to give an overall time for the update loop
    updateAll_times = UpdateData(FKreptFactor)
    updateEndTime = time.time()

    noConstraintTimesAll = report_times(updateAll_times)

    #***Remove and then Rebuild the Tables with constraints***
    Table.RemoveAllTables(client, True) #True to wait for confirmation

    Table.SetVerifyConstraints(True)
    Table.UseFKTables(True)

    BuildTables()
    InsertData(FKreptFactor)

updateStartTime = time.time() #updateStartTime/EndTime only use to give an overall time for the update loop
updateAll_times = UpdateData(FKreptFactor)
updateEndTime = time.time()

withConstraintTimesAll = report_times(updateAll_times)

benchdata.rcrd_data("Total Update Time: {} seconds, 2 tables".format(updateEndTime - updateStartTime))
report_statistics(withConstraintTimesAll, noConstraintTimesAll)

benchdata.close()
del benchdata

if sys.argv[1] == "AS":
    #Remove all tables and rebuild them for the delete.
    Table.RemoveAllTables(client, True) #True to wait for confirmation

#      ==============================================
#         Delete Section
#      ==============================================
benchdata =drutils("Schema3_Delete_expt_"+sys.argv[1]+"_"+NumberRowsStr+"_","b")

if sys.argv[1] == "AS":
    Table.SetVerifyConstraints(True)
    Table.UseFKTables(True)

    BuildTables()
    InsertData(FKreptFactor)

deleteStartTime = time.time() #deleteStartTime/EndTime only use to give an overall time for the delete loop
deleteAll_times = DeleteData(FKreptFactor)
deleteEndTime = time.time()

withConstraintTimesAll = report_times(deleteAll_times)

Table.RemoveAllTables(client, True) #True to wait for confirmation

if sys.argv[1] == "AS":
    Table.SetVerifyConstraints(False)
    Table.UseFKTables(False)

    BuildTables()
    InsertData(FKreptFactor)

    deleteStartTime = time.time() #deleteStartTime/EndTime only use to give an overall time for the delete loop
    deleteAll_times = DeleteData(FKreptFactor)
    deleteEndTime = time.time()

    noConstraintTimesAll = report_times(deleteAll_times)

benchdata.rcrd_data("Total Delete Time: {} seconds, 2 tables".format(deleteEndTime - deleteStartTime))
report_statistics(withConstraintTimesAll, noConstraintTimesAll)

benchdata.close()
del benchdata

# Finally, remove the leftover tables
Table.RemoveAllTables(client, True) #True to wait for confirmation

client.close()