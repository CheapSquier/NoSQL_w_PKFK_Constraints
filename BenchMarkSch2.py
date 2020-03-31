#!/usr/bin/python3
# coding: utf-8

# In[1]:

RunActualDB = True
from Private_Pkgs.DataRecordingUtils import drutils
import sys
import json
import datetime
import time
import random
import statistics
import pprint
import math

print("Current Python version: ", sys.version_info[0],".",sys.version_info[1],".",sys.version_info[2])

_useAllTimes = False
_print_raw = False
_DB_mode = sys.argv[1]
_SchemaNumber = 2
if _DB_mode != "AS" and _DB_mode != "SQL":                       # FIXME Is this code redundant or incomplete?
    print("ERROR: MUST SPECIFY AS OR SQL MODE AS 1ST PARAMETER")
    exit()
# Import the Table class used to build Table schema. Use the class specific to AeroSpike or SQL
if _DB_mode == "AS":
    # Aerospike won't be a available on a windows machine so don't try to load the API
    import aerospike
    from Private_Pkgs.AS_Table import Table # Defines Table class used to build Table schema for AeroSpike
else:
    import mysql.connector
    from mysql.connector import errorcode
    from Private_Pkgs.SQL_Table import Table

# In[2]:

def BuildTables(database_namespace):
    TableA = Table(database_namespace, "Table2A")
    TableA.AddCol("PKcol_TabA")
    TableA.AddCol("Data_Column")
    TableA.AddPK("PKcol_TabA")
    if TableA.VerifyFKrefsCompletePK(): # In SQL mode, this will create the table
        if _DB_mode == "AS":
            print(TableA.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableA.TableName, "has been created in the SQL database")

    TableB = Table(database_namespace, "Table2B")
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
    #Setup a random, no repeating sequence
    random.seed(42)
    row_seqTableA = list(range(int(NumberRows/FKreptFactor), NumberRows)) # Lower values will be referenced by Table B FK, so can't delete
    row_seqTableB = list(range(0, NumberRows))
    random.shuffle(row_seqTableA)
    random.shuffle(row_seqTableB)

    for idx in range(0, int(NumberRows/2)):
        FKindex = math.floor(row_seqTableB[idx]/NumberHashesPerFK)
        A_Row = []
        B_Row = []
        A_Row.append(row_seqTableA[idx]) #The PK
        B_Row.append(row_seqTableB[idx]) #The PK for Table B
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
        benchdata.rcrd_data("Statistics for: " + withConKey)
        benchdata.rcrd_data("\tWith Constraints:\t\tWithout Constraints\t\tPercent Difference, {} rows".format(noConBenchTime[0]))
        benchdata.rcrd_data("MEDIAN:\t{} \t\t{} ms\t\t{}".format(withConBenchTime[2],noConBenchTime[2],
             (withConBenchTime[2]-noConBenchTime[2])/noConBenchTime[2]*100))
        benchdata.rcrd_data("MEAN:\t{} \t\t{} ms\t\t{}".format(withConBenchTime[3],noConBenchTime[3],
             (withConBenchTime[3]-noConBenchTime[3])/noConBenchTime[3]*100))
        benchdata.rcrd_data("STDEV:\t{} \t\t{} ms\t\t{}".format(withConBenchTime[4],noConBenchTime[4],
             (withConBenchTime[4]-noConBenchTime[4])/noConBenchTime[4]*100))
        benchdata.rcrd_data()
    return

def logCSVdata(recordedTimes, operation, constraints):
    global NumberRows
    global FKreptFactor

    # listOfTimes is a dict of lists. The keys are the dict names, so we'll have to index thru to add the table names
    # for the calls to log_csv
    #From Module: X             X                             X          X 
    #From call:                               X      X                                X        |   list of times |
    #              DB      ,   schema, ,  operation , table, rows ,   FK rep    ,   constrt  , mean, median, stdev  
    #            AS|SQL    ,    1-3    , Ins|Upd|Del, str,    nr  ,     FKrF     , True|False  

    #
    for tableName in recordedTimes:
        csv_list = [_DB_mode, _SchemaNumber, operation, tableName, NumberRows, FKreptFactor, constraints]
        csv_list.append(statistics.mean(recordedTimes[tableName]))
        csv_list.append(statistics.median(recordedTimes[tableName]))
        csv_list.append(statistics.stdev(recordedTimes[tableName]))
        # Now append a timestamp
        csv_list.append(str("{:%Y-%m-%d_%H%M%S}".format(datetime.datetime.now())))
        csvdata.log_csv2file(csv_list)
    return 

Table.SetDebugMode(False)
Table.GetDebugMode()

# ***Open communication with the database***
if _DB_mode == "AS":
    # Specify the IP addresses and ports for the Aerospike cluster
    config = {
    'hosts': [ ('18.222.211.167', 3000), ('3.16.180.184', 3000) ]
    }
    # Create a client and connect it to the cluster
    print("RunActualDB is", RunActualDB)
    print("Benchmarking with AeroSpike, Schema 2")
    database="test_DB_SEF"
    try:
        client = aerospike.client(config).connect()
        #Be sure to tell the Table class the name of the client it's talking to
        Table.SetTableClient(client)
    except:
        print("failed to connect to the cluster with", config['hosts'])
        sys.exit(1)
if _DB_mode == "SQL":
    client = mysql.connector.connect(
    host="3.16.81.134",
    user="demouser",
    passwd="DrBajaj2*",
    database="test_DB_SQL"
    )
    database = "test_DB_SQL"
    '''-Args for local SQL database
        host="127.0.0.1",
        user="root",
        database="test_DB_SQL"
       -Args for AWS SQL database
        host="18.191.176.248",
        user="demouser",
        passwd="DrBajaj2*",
        database="test_DB_SQL"
    '''
    sqlCursor = client.cursor(buffered=True)
    Table.SetTableClient(client, sqlCursor)
    print("Benchmarking with MySQL, Schema 2")



# ==============================================
#    Start of Main Program Loop
# ==============================================
csvdata = drutils("NoSQLvsSQL_bench_data","f", "csv")

if len(sys.argv) == 1+1: # Only specified the DB type, so input params
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
benchdata =drutils("Schema2_Insert_expt_"+_DB_mode+"_"+NumberRowsStr+"_","n") # if the mode is n, there wil be no logging.
                                                                              # Change to f, s, or b if logging is needed

Table.SetVerifyConstraints(True)
Table.UseFKTables(True)

# ***Build the tables***
BuildTables(database)

# ***Insert priming data, collect Insert times***
insertStartTime = time.time() #insertStartTime/EndTime only use to give an overall time for the insert loop
insertAll_times = InsertData(FKreptFactor)
insertEndTime = time.time()

# ### Calculate Time Statistics (Checking constraints during Insert)
withConstraintTimesAll = report_times(insertAll_times)
logCSVdata(insertAll_times, "Ins" , Table.GetVerifyConstraints())
noConstraintTimesAll = []
if _DB_mode == "AS":
    # ### Remove Benchmark Tables
    Table.RemoveAllTables(client, True) #True to wait for confirmation

    # ### Rebuild Tables and re-insert data, after disabling Constraints

    Table.SetVerifyConstraints(False)
    Table.UseFKTables(False)

    BuildTables(database)
    insertAll_times = InsertData(FKreptFactor)
    noConstraintTimesAll = report_times(insertAll_times)
    logCSVdata(insertAll_times, "Ins" , Table.GetVerifyConstraints())
    report_statistics(withConstraintTimesAll, noConstraintTimesAll) # Only call this for Aerospike since that's 
                                                                    # where we have a constraint vs no constraint

# ### Calculate Time Statistics (No constraint verification during Insert)    Close the data object
benchdata.rcrd_data("Total Insert Time: {} seconds, 2 tables".format(insertEndTime - insertStartTime))
benchdata.close()
del benchdata

#      ==============================================
#         Update Section
#      ==============================================
benchdata =drutils("Schema2_Update_expt_"+_DB_mode+"_"+NumberRowsStr+"_","n")

noConstraintTimesAll = []
if _DB_mode == "AS":
    ### We already have tables that do NOT use constraints or FK Tables ###
    #Therefore, the first run of Update and Delete will be with ***No*** constraints
    updateStartTime = time.time() #updateStartTime/EndTime only use to give an overall time for the update loop
    updateAll_times = UpdateData(FKreptFactor)
    updateEndTime = time.time()

    noConstraintTimesAll = report_times(updateAll_times)
    logCSVdata(updateAll_times, "Upd" , Table.GetVerifyConstraints())
    #***Remove and then Rebuild the Tables with constraints***
    Table.RemoveAllTables(client, True) #True to wait for confirmation

    Table.SetVerifyConstraints(True)
    Table.UseFKTables(True)

    BuildTables(database)
    InsertData(FKreptFactor)

updateStartTime = time.time() #updateStartTime/EndTime only use to give an overall time for the update loop
updateAll_times = UpdateData(FKreptFactor)
updateEndTime = time.time()

withConstraintTimesAll = report_times(updateAll_times)
logCSVdata(updateAll_times, "Upd" , Table.GetVerifyConstraints())
if _DB_mode == "AS": # Only need to call this for AS since its with/without constraints
    report_statistics(withConstraintTimesAll, noConstraintTimesAll)

benchdata.rcrd_data("Total Update Time: {} seconds, 2 tables".format(updateEndTime - updateStartTime))
benchdata.close()
del benchdata

if _DB_mode == "AS":
    #Remove all tables and rebuild them for the delete.
    Table.RemoveAllTables(client, True) #True to wait for confirmation

#      ==============================================
#         Delete Section
#      ==============================================
benchdata =drutils("Schema2_Delete_expt_"+_DB_mode+"_"+NumberRowsStr+"_","n")

if _DB_mode == "AS":
    Table.SetVerifyConstraints(True)
    Table.UseFKTables(True)

    BuildTables(database)
    InsertData(FKreptFactor)

deleteStartTime = time.time() #deleteStartTime/EndTime only use to give an overall time for the delete loop
deleteAll_times = DeleteData(FKreptFactor)
deleteEndTime = time.time()

withConstraintTimesAll = report_times(deleteAll_times)
logCSVdata(deleteAll_times, "Del" , Table.GetVerifyConstraints())
Table.RemoveAllTables(client, True) #True to wait for confirmation

if _DB_mode == "AS":
    Table.SetVerifyConstraints(False)
    Table.UseFKTables(False)

    BuildTables(database)
    InsertData(FKreptFactor)

    deleteStartTime = time.time() #deleteStartTime/EndTime only use to give an overall time for the delete loop
    deleteAll_times = DeleteData(FKreptFactor)
    deleteEndTime = time.time()

    noConstraintTimesAll = report_times(deleteAll_times)
    logCSVdata(deleteAll_times, "Del" , Table.GetVerifyConstraints())
    report_statistics(withConstraintTimesAll, noConstraintTimesAll) # Only call this for Aerospike since that's 
                                                                    # where we have a constraint vs no constraint
benchdata.rcrd_data("Total Delete Time: {} seconds, 2 tables".format(deleteEndTime - deleteStartTime))

# Finally, remove the leftover tables, close files and DB client
Table.RemoveAllTables(client, True) #True to wait for confirmation

csvdata.close()
del csvdata
benchdata.close()
del benchdata
client.close()
del client