#!/usr/bin/python3
# coding: utf-8

# In[1]:

RunActualDB = True
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

def BuildSchema1(database_namespace):
    TableA = Table(database_namespace, "Table1A")
    TableA.AddCol("PKcol_TabA")
    TableA.AddCol("Data_Column")
    TableA.AddPK("PKcol_TabA")
    if TableA.VerifyFKrefsCompletePK(): # In SQL mode, this will create the table
        if _DB_mode == "AS":
            print(TableA.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableA.TableName, "has been created in the SQL database")

    TableB = Table(database_namespace, "Table1B")
    TableB.AddCol("PKcol_TabB")
    TableB.AddCol("FK_BtoA")
    TableB.AddCol("Data_Column")
    TableB.AddPK("PKcol_TabB")
    TableB.AddFK("FK_BtoA", "Table1A", "PKcol_TabA")
    if TableB.VerifyFKrefsCompletePK():
        if _DB_mode == "AS":
            print(TableB.TableName, "has complete FK to PK references (or none exist in table)")
        else:
            print(TableB.TableName, "has been created in the SQL database")

def BuildSchema2(database_namespace):
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
        
    All_times["Table1A"] = A_times
    All_times["Table1B"] = B_times
    return(All_times)

Table.SetDebugMode(False)
Table.GetDebugMode()

# ***Open communication with the database***
if sys.argv[1] == "AS":
    # Specify the IP addresses and ports for the Aerospike cluster
    config = {
    'hosts': [ ('18.222.211.167', 3000), ('3.16.180.184', 3000) ]
    }
    # Create a client and connect it to the cluster
    print("RunActualDB is", RunActualDB)
    print("Benchmarking with AeroSpike, Schema 1")
    database="test_DB_SEF"
    try:
        client = aerospike.client(config).connect()
        #Be sure to tell the Table class the name of the client it's talking to
        Table.SetTableClient(client)
    except:
        print("failed to connect to the cluster with", config['hosts'])
        sys.exit(1)
if sys.argv[1] == "SQL":
    client = mysql.connector.connect(
    host="18.191.176.248",
    user="demouser",
    passwd="DrBajaj2*",
    database="test_DB_SQL"
    )
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
    print("Benchmarking with MySQL, Schema 1")

# ==============================================
#    Start of Main Program Loop
# ==============================================
Table.SetVerifyConstraints(True)
Table.UseFKTables(True)

# Test 1: Insert a row into a table with an already existing PK
BuildSchema1(database)
TableA = Table._registry[0]
TableB = Table._registry[1]
TableA.Insert([1, "datadata"])
TableA.Insert([2, "datadata"]) # This should work
print(">>> Test 1: Expect an Insert Failed error, non-unique PK value")
TableA.Insert([1, "datadata"]) # This should fail

# Should now have Table 1A with 2 rows, PK = 1 and PK = 2. Table 1B is still empty

# Test 2: Insert a row into a table where the referenced FK value does not exist
TableB.Insert([1, 1, "datadata"])
TableB.Insert([2, 1, "datadata"]) # This should work
print(">>> Test 2: Expect an Insert Failed error, non-existant FK value")
TableB.Insert([3, 999, "datadata"]) # This should fail

# Test 3a:
TableA.Delete([2]) # This should work. This FK value isn't referenced by another table.
print(">>> Test 3: Expect an Delete Failed error, this table is referenced in TableB")
TableA.Delete([1])

# Test 3a: (How many records are removed)
# At this point, should have 4 records. 1 in Table 1A, 2 in Table 1B, 1 the Table 1B's FK Table
print(">>> Test 3a: Expect 4 records removed")
Table.RemoveAllTables(client, True) #True to wait for confirmation

# Test 4: Insert a row into a table with an already existing PK (but fails because of the FK in the composite PK)
BuildSchema2(database)
TableA = Table._registry[0]
TableB = Table._registry[1]
TableA.Insert([1, "datadata"])
TableA.Insert([2, "datadata"]) 
TableA.Insert([3, "datadata"]) 
TableB.Insert([1, 1, "datadata"])
TableB.Insert([1, 2, "datadata"]) # This should work
print(">>> Test 2: Expect an Insert Failed error, non-unique composite PK")
TableB.Insert([1, 1, "datadata"]) # This should fail, repeated PK

# Test 4a: (How many records are removed)
# At this point, should have 7 records. 3 in Table 2A, 2 in Table 2B, 2 the Table 1B's FK Table
print(">>> Test 4a: Expect 7 records removed")
Table.RemoveAllTables(client, True) #True to wait for confirmation

# Test 5: FK does not reference a complete PK
TableA = Table(database, "Table4A")
TableA.AddCol("PKcol1")
TableA.AddCol("PKcol2")
TableA.AddCol("Data_Column")
TableA.AddPK("PKcol1")
TableA.AddPK("PKcol2")
if TableA.VerifyFKrefsCompletePK(): # This should pass
    if _DB_mode == "AS":
        print(TableA.TableName, "has complete FK to PK references (or none exist in table)")
    else:
        print(TableA.TableName, "has been created in the SQL database")

TableB = Table(database, "Table4B")
TableB.AddCol("PKcol1")
TableB.AddCol("FK1_BtoA")
TableB.AddCol("FK2_BtoA")
TableB.AddCol("Data_Column")
TableB.AddPK("PKcol1")
TableB.AddFK("FK1_BtoA", "Table4A", "PKcol1")
TableB.AddFK("FK2_BtoA", "Table4A", "PKcol2")

if TableB.VerifyFKrefsCompletePK(): # This should pass since FK refs complete PK
    if _DB_mode == "AS":
        print(TableB.TableName, "has complete FK to PK references (or none exist in table)")
    else:
        print(TableB.TableName, "has been created in the SQL database")

TableC = Table(database, "Table4C")
TableC.AddCol("PKcol1")
TableC.AddCol("FK1_CtoA")
TableC.AddCol("Data_Column")
TableC.AddPK("PKcol1")
TableC.AddFK("FK1_CtoA", "Table4A", "PKcol1")

print(">>> Test 5: Expect an incomplete FK to PK reference error")
if TableC.VerifyFKrefsCompletePK(): # This should fail since only 1 PK col in the composite PK is referenced.
    if _DB_mode == "AS":
        print(TableC.TableName, "has complete FK to PK references (or none exist in table)")
    else:
        print(TableC.TableName, "has been created in the SQL database")

print(">>> Test 5a: Expect 0 records removed (since no data was inserted)")
Table.RemoveAllTables(client, True) #True to wait for confirmation

# Test 6: FK references a Column or Table that does not exist.
TableA = Table(database, "Table1A")
TableA.AddCol("PKcol_TabA")
TableA.AddCol("Data_Column")
TableA.AddPK("PKcol_TabA")
if TableA.VerifyFKrefsCompletePK(): # In SQL mode, this will create the table
    if _DB_mode == "AS":
        print(TableA.TableName, "has complete FK to PK references (or none exist in table)")
    else:
        print(TableA.TableName, "has been created in the SQL database")

TableB = Table(database, "Table1B")
TableB.AddCol("PKcol_TabB")
TableB.AddCol("FK_BtoA")
TableB.AddCol("Data_Column")
TableB.AddPK("PKcol_TabB")
print(">>> Test 6: Non-existant table reference error")
TableB.AddFK("FK_BtoA", "TableThatShallNotBeNamed", "PKcol_TabA")
print(">>> Test 6a: Non-existant column reference error")
TableB.AddFK("FK_BtoA", "Table1A", "ColumnThatShallNotBeNamed")

print(">>> Test 6b: Expect 0 records removed (since no data was inserted)")
# Finally, remove any leftover tables
Table.RemoveAllTables(client, True) #True to wait for confirmation

client.close()