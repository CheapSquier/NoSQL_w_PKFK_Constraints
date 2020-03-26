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
    TableA = Table("test_DB_SQL", "Table2A")
    TableA.AddCol("PKcol_TabA")
    TableA.AddCol("Data_Column")
    TableA.AddPK("PKcol_TabA")
    if TableA.VerifyFKrefsCompletePK(): # In SQL mode, this will create the table
        if _DB_mode == "AS":
            print(TableA.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableA.TableName, "has been created in the SQL database")

    TableB = Table("test_DB_SQL", "Table2B")
    TableB.AddCol("PKcol_TabB")
    TableB.AddCol("FK_BtoA")
    TableB.AddCol("Data_Column")
    TableB.AddPK("PKcol_TabB")
    TableB.AddPK("FK_BtoA")  # *** This is the only difference between Schema 1 and Schema 2
    TableB.AddFK("FK_BtoA", "Table2A", "PKcol_TabA")
    if TableB.VerifyFKrefsCompletePK():
        if _DB_mode == "AS":
            print(TableB.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableB.TableName, "has been created in the SQL database")

def InsertData(NumberHashesPerFK):
    insertA_times = []
    insertB_times = []
    insertAll_times = {}
    TableA = Table._registry[0]
    TableB = Table._registry[1]
    print(TableA.TableName)
    print(TableB.TableName)
    # Insert rows for Table A
    for rowNum in range(0,int(NumberRows)):
        thisRow =[]
        thisRow.append(rowNum) #The PK
        thisRow.append("datadata") # constant length
        t0 = time.time()
        TableA.Insert(thisRow)
        t1 = time.time()
        insertA_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
    # Insert rows for Table B    
    for rowNum in range(0,int(NumberRows)):
        FKindex = math.floor(rowNum/NumberHashesPerFK)
        thisRow =[]
        thisRow.append(rowNum) #The PK
        thisRow.append(FKindex) #The FK
        thisRow.append("datadata") # constant length
        t0 = time.time()
        TableB.Insert(thisRow)
        t1 = time.time()
        insertB_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
    insertAll_times["Table2A"] = insertA_times
    insertAll_times["Table2B"] = insertB_times
    return(insertAll_times)

def UpdateData(NumberHashesPerFK):
    A_times = []
    B_times = []
    All_times = {}
    TableA = Table._registry[0]
    TableB = Table._registry[1]
    print(TableA.TableName)
    print(TableB.TableName)
    #Setup a random, non repeating sequence
    random.seed(42)
    row_seq = list(range(0, NumberRows))
    random.shuffle(row_seq)

    for idx in range(0, int(NumberRows)):
        thisRow =[]
        thisRow.append(row_seq[idx]) #The PK
        thisRow.append("dataXXXX") # constant length
        t0 = time.time()
        TableA.Update(thisRow)
        #print("Table2A update: ", thisRow)
        t1 = time.time()
        A_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
    for idx in range(0, int(NumberRows)):
        FKindex = math.floor(row_seq[idx]/NumberHashesPerFK)
        thisRow =[]
        thisRow.append(row_seq[idx]) #The PK
        thisRow.append(FKindex) #The FK (ascending.. mixing ascending and descending FK in insert and update can trigger FK constraints)
        thisRow.append("dataXXXX") # constant length
        t0 = time.time()
        TableB.Update(thisRow)
        #print("Table2B update: ", thisRow)
        t1 = time.time()
        B_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
    All_times["Table2A"] = A_times
    All_times["Table2B"] = B_times
    return(All_times)

def DeleteData(NumberHashesPerFK):
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
        FKindex = math.floor(row_seq[idx]/NumberHashesPerFK)
        A_Row = []
        B_Row = []
        A_Row.append(row_seq[idx]) #The PK
        B_Row.append(row_seq[idx]) #The PK for Table B
        B_Row.append(FKindex) #The 2nd PKcol for Table B (also an FK) # *** Difference in execution for Schema1 and Schema2
        t0 = time.time()
        TableB.Delete(B_Row)
        t1 = time.time()
        B_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
        t0 = time.time()
        TableA.Delete(A_Row)
        t1 = time.time()
        A_times.append((t1 - t0) * 1000) # Times are in S so *1000 makes units mS
        
    All_times["Table2A"] = A_times
    All_times["Table2B"] = B_times
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
benchdata =drutils("Schema2_Insert_expt_"+sys.argv[1]+"_"+NumberRowsStr+"_","b")

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
benchdata =drutils("Schema2_Update_expt_"+sys.argv[1]+"_"+NumberRowsStr+"_","b")

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
benchdata =drutils("Schema2_Delete_expt_"+sys.argv[1]+"_"+NumberRowsStr+"_","b")

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