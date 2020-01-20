#!/usr/bin/env python
# coding: utf-8
import sys
import json
from sortedcontainers import SortedList
import mysql.connector

class Table:
    'Class that implements the same API framework as AS_Table.py, but for use with a MySQL DB'

    #Class variables   
    _registry = []
    _VerifyConstraints = True
    _UseFKTables = True
    _CurrentClient = None
    _Debug = True
    #Class methods
    @staticmethod
    def SetDebugMode(DebugTF):
        if DebugTF: Table._Debug = True
        else: Table._Debug = False
    @staticmethod
    def GetDebugMode():
        if Table._Debug == True: print("Debug mode is On")
        else: print("Debug mode is Off")
    @staticmethod
    def SetVerifyConstraints(EnabledTF):
        if EnabledTF: Table._VerifyConstraints = True
        else: Table._VerifyConstraints = False
    @staticmethod
    def GetVerifyConstraints():
        if Table._VerifyConstraints == True:
            print("Verifying Constraints is ENABLED")
            return True
        else:
            print("Verifying Constraints is DISABLED")
            return False
    @staticmethod
    def UseFKTables(EnabledTF):
        if EnabledTF: Table._UseFKTables = True
        else: Table._UseFKTables = False
    @staticmethod
    def GetUseFKTables():
        if Table._UseFKTables == True:
            print("Use FK Tables is ENABLED")
            return True
        else:
            print("Use FK Tables is is DISABLED")
            return False
    @staticmethod
    def SetTableClient(thisClient):
        Table._CurrentClient = thisClient
        print("Info: Table Client Set, connection state is: ", Table._CurrentClient.is_connected())
    @staticmethod
    def AddTableRowHash(TableName, hashkey):
        refTable = None
        for indexTable in Table._registry: #Get the table object reference
            print("indexTab:", indexTable.TableName, "table passed:", TableName)
            if (indexTable.TableName == TableName):
                refTable = indexTable
                break
        if refTable == None:
            print("Error: ", TableName, " not found in Table _registry!")
            return False
        #refTable.Rows.append(hashkey)
        refTable.Rows.add(hashkey) # Sorted
        return True
    @staticmethod
    def RemoveAllTables(thisClient, Force = False):
        if Force == False:
            confirmation = input("Do you want to remove all tables? (yes to confirm): ")
            if confirmation != "yes":
                print("Note: Ignoring call to RemoveAllTables")
                return False
        records_removed = 0
        for indexTable in Table._registry: #Get the table object reference
            print("Removing rows in Table ", indexTable.TableName)
            for thisRowHash in indexTable.Rows:
                key = (indexTable.NameSpace, indexTable.TableName, None, thisRowHash)
                try:
                    thisClient.remove(key)
                    records_removed+=1
                except Exception as e:
                    print("error: {0}".format(e), sys.stderr)
                    print("Error occured after ", records_removed, "records removed")
        Table._registry.clear()
        print("Note: ", records_removed, "records removed")
        return
    @staticmethod
    def RemoveDBStructure(thisClient, DBName):
        confirmation = input("Do you want to remove the DB structure? (yes to confirm): ")
        if confirmation != "yes":
            print("Note: Ignoring call to RemoveDBStructure")
            return False
        try:
            hashkey = thisClient.get_key_digest(Table._NameSpace, DBName, "Structure")
            key = (Table._NameSpace, DBName, None, hashkey)
            thisClient.remove(key)
        except Exception as e:
            print("Error attempting to remove ", DBName, "structure record.")
            print("error: {0}".format(e), sys.stderr)
        return

    #constructor
    def __init__(self, namespace, name):
        self.NameSpace = namespace
        self.TableName = name
        self.Columns = [] # List of colNames for this Table.
        self.PK = [] # List of colNames that are to be used as PKs for this Table.
        self.FK = [] # List of dicts composed of {'colName': someColName, 'refTable': <obj ref>, 'refColName': otherColName}
        self.FKnames = [] # simplified list of FK colNames for this Table
        self.FKtables = {} # dict with FK table names as keys. Values are the FKTable objects
        self.Rows = SortedList() # sortedcontainers version of a List. Values are hashkeys of DB records in this Table.
        self._registry.append(self) 
    def AddCol(self, colName):
        if colName not in self.Columns:
            self.Columns.append(colName)
            return True
        else:
            print("AddCol with column name ", colName, "ignored: Name already.")
            return False
    def AddPK(self, colName):
        if colName in self.Columns:
            self.PK.append(colName)
            return True
        else:
            print("AddPK with column name ", colName, " ignored: Not a Column in Table.")
            return False
    def AddFK(self, colName, refTableName, refColName):
        #Verify FK name exists in table
        if colName not in self.Columns:
            print("AddFK with column name ", colName, " ignored: Not a Column in Table.")
            return False
        #Verify Ref table exists
         #Determine refTable (object) from refTableName
        refTable = None
        for indexTable in Table._registry:
            if (indexTable.NameSpace == self.NameSpace) and (indexTable.TableName == refTableName):
                refTable = indexTable
        if not refTable:
            print("AddFK with ref Table name ", refTable, " ignored: Not a Table in Namespace.")
            return False
        #Verify Ref column exits in Ref table
        if refColName in refTable.Columns:
            #Add FK information to FK[] list of dict
            FKdict = {"colName":colName, "refTable":refTable, "refColName":refColName}
            self.FK.append(FKdict)
            #Create an FK Table for this FK, regardless of UseFKTables setting.
            self.FKtables[str(colName+"_FKTable")] = Table("test_DB_SEF", colName+"_FKTable")
            self.FKtables[str(colName+"_FKTable")].AddCol(colName+"_FKPK")
            self.FKtables[str(colName+"_FKTable")].AddCol("TableHashes")
            self.FKtables[str(colName+"_FKTable")].AddPK(colName+"_FKPK")
            return True
        else:
            print("AddFK with ref Column name ", refColName, " ignored: Not a Column in ", refTable)
            return False
    def VerifyStructure(self, listOfValues):
        if len(listOfValues) != len(self.Columns):
            print("VerifyStructure with attributes ", listOfValues, " failed: Incorrect number of attributes.")
            return False
        else:
            return True
    def VerifyUniquePKval(self, listOfPKValues):
        #Need to be able to handle composite PK values.
        #1. Verify the # of values in listOfValues matches the # of values in self.PK[]. False = Error
        #  -VerifyStructure checks length of attribs for entire table, this step just checks #PKs, which
        #   means calling function has to isolate PK vals from rest of vals
        if type(listOfPKValues) != list:
            listOfPKValues = [listOfPKValues]
        if len(listOfPKValues) != len(self.PK):
            print("VerifyUniquePK with PKs ", listOfPKValues, " failed: Incorrect number of PK attributes.")
            return False
        #2. Verify that listOfValues is NOT in the list of PK values for this Table
        #Convert to hashkey
        hashkey = self.getHashKey(str(listOfPKValues))
        if hashkey in self.Rows:
            if Table._Debug: 
                print(listOfPKValues, " exists in the following row (VerifyUniquePKval):")
                print(self.Read_PK(listOfPKValues))
            return False
        return True
    def VerifyFKvalExists(self, listOfFKValues): # Suspect this may not work for multiple FK values or FK refs multivariate PK
        if len(listOfFKValues) != len(self.FK):
            print("VerifyFKvalExists with FKs ", listOfValues, " failed: Incorrect number of FK attributes.")
            return False
        #1. When verifying FKs, need to group by refTable.
        #2. For the set of FKs for each refTable, get the hash and see if it exists in the refTables list of Rows (hashes)
        #3. When getting the hash, the PK values used will have to be in the same order as specified by the table structure
        #  -FKs are not necessarily in the same order as their PK counterparts as defined in the tables
        #Orig: insertDict = dict(zip(self.Columns, listOfFKValues))
        insertDict = dict(zip(self.FKnames, listOfFKValues))
        if Table._Debug: print("VerifyFK insertDict", insertDict)
        # if this works, Replace the next 5 lines with self.refTableList, created in VerifyFKrefsCompletePK
        refTableList = []
        # First, make a list of all referenced tables (it's not a 1:1 mapping of PK/FK to tables)
        for thisFK in self.FK:
            if thisFK["refTable"] not in refTableList:
                refTableList.append(thisFK["refTable"])
        for thisRefTable in refTableList:
            PKValList = []
            #Each ref'd table should have a value (named by FK) for each of its PKs
            for thisPK in thisRefTable.PK:
                #find the value of the corresponding FK in self Table, add it to the PKValList
                for thisFK in self.FK:
                    if Table._Debug: print("VerifyFKvalExists for refTable, PK, FK: ", thisRefTable.TableName, thisPK, thisFK)
                    if thisFK["refColName"] == thisPK:
                        PKValList.append(insertDict[thisFK["colName"]])
                        if Table._Debug: print("VerifyFKvalExists, PKValList: ", PKValList)
            #Now PKValList should have a list of values that correspond to PKs in the refTable, in the same order
            #Check if they exist by looking for a fail on VerifyUniquePKval
            unique = thisRefTable.VerifyUniquePKval(PKValList)
            if unique:
                print("VerifyFKvalExists with FKs ", listOfFKValues, "failed, those values not in", thisRefTable.TableName)
                return False
        #If we get to here, all ref tables and PKs should have been checked
        return True
    def VerifyFKrefsCompletePK(self):
        if Table._VerifyConstraints:
            if len(self.FK) == 0:
                print(self.TableName, "has no FK constraints to verify.")
                return True
            refTableList = []
            # First, make a list of all referenced tables (it's not a 1:1 mapping of PK/FK to tables)
            for thisFK in self.FK:
                if thisFK["refTable"] not in refTableList:
                    refTableList.append(thisFK["refTable"])
            if Table._Debug: print("refTables in FK list:", refTableList)
            # Next, for each table in that list, check all of its PKs are listed in this table's FK list
            for thisRefTable in refTableList:
                PKfound = 0
                for thisPK in thisRefTable.PK:
                    for thisFK in self.FK:
                        if thisPK == thisFK["refColName"]:
                            PKfound+=1
                            print("thisPK == thisFK refColName: ",thisPK, thisFK["refColName"], "with #PKs found", PKfound)
                if PKfound != len(thisRefTable.PK):
                    print(self.TableName, "has FK refs to table ", thisRefTable.TableName," but does not ref", thisPK)
                    return False
            # If we're here, FK/PK refs are ok. Make a FKnames list
            for FKdict in self.FK:
                self.FKnames.append(FKdict["colName"])
        return True
    def Report(self):
        print("NameSpace    : ", self.NameSpace)
        print("Table Name   : ", self.TableName)
        print("Columns      : ", self.Columns, "Total: ", len(self.Columns))
        print("PK Columns   : ", self.PK, "Total: ", len(self.PK))
        print("FK Columns   : ", self.FK, "Total: ", len(self.FK))
        print("FK Col Names : ", self.FKnames, "Total: ", len(self.FKnames))
        print("FK Tables    : ", self.FKtables)
        print("1st 3 Rows of ", len(self.Rows), " rows total:")
        for thisRow in self.Rows:
            if self.Rows.index(thisRow) >= 3: break
            print("Application storage ", thisRow)
    def Insert(self, listOfValues):
        currentPKvals = []
        #Assume we've already verified PK name is in Columns
        #1st, build a list of PK values based on what's in listOfValues
        for PKcol in self.PK:
            #Take the index of PK from Columns and use that to grab same index from listOfValues
            currentPKvals.append(listOfValues[self.Columns.index(PKcol)])
        if Table._Debug: print("PKvals from Insert: ", currentPKvals, "for Table: ", self.TableName)
        if Table._VerifyConstraints:
            if not self.VerifyStructure(listOfValues):
                print("Insert not successful, VerifyStructure failed")
                return False
            if not self.VerifyUniquePKval(currentPKvals):
                print("Insert failed! PK values not unique!")
                return False 
            #if Table has FKkeys, VerifyFKs exist in the refTable
            if len(self.FK) > 0:   
                currentFKvals = []
                for FKcol in self.FK:
                    currentFKvals.append(listOfValues[self.Columns.index(FKcol["colName"])])
                if Table._Debug: print("FKvals from Insert: ", currentFKvals, "for Table: ", self.TableName)
                if not self.VerifyFKvalExists(currentFKvals):
                    print("Insert failed! FK values don't exist in referenced Table!")
                    return False 
        #if all of the verifies are successful, insert the data.
        #  -JSONize it (which means forming a dictionary by combining listOfValues with Columns names)
        string4JSON = dict(zip(self.Columns, listOfValues))
        if Table._Debug:
            jsonString = json.dumps(string4JSON)
            print("JSON string:", jsonString)
        #Get the HK from the put.
        # Records are addressable via a tuple of (namespace, set, key)
        # key = (self.NameSpace, self.TableName, str(currentPKvals))
        # but, can also pre-build the key and store it using the key digest
        hashkey = self.getHashKey(str(currentPKvals))
        key = (self.NameSpace, self.TableName, None, hashkey)
        try:
          # Write a record
          #client.put(key, jsonString)
          Table._CurrentClient.put(key, string4JSON)
        except Exception as e:
            print("error: {0}".format(e), sys.stderr)
        #self.Rows.append(hashkey) # reglular List
        self.Rows.add(hashkey)     # SortedList
        #And don't forget to update the FKTables if there are any FKs in this table
        if len(self.FK) > 0 and Table._UseFKTables:   
            for FKcol in self.FK:
                currentFKval = []
                currentFKval.append(listOfValues[self.Columns.index(FKcol["colName"])])
                if Table._Debug: 
                    print("FKval from Insert: ", currentFKval, "for Table: ", self.TableName)
                    print("FKTable is: ", self.FKtables[str(FKcol["colName"]+"_FKTable")].TableName)
                self.FKtables[str(FKcol["colName"]+"_FKTable")].Insert4FK(currentFKval, hashkey) #Verify, key or hashkey
            #if not self.VerifyFKvalExists(currentFKvals): # ??? Why is this here?
        return True
    def Insert4FK(self, FKValue, parent_keyValue):
        if Table._Debug: print("**** The parent_keyValue is", parent_keyValue)
        #We'll need the key regardless of whether or not this FKValue already exists
        hashkey = self.getHashKey(str(FKValue)) #Setting the PK
        key = (self.NameSpace, self.TableName, None, hashkey)
        #Check and see if the FKValue is among the PK values for this Table.
        if self.VerifyUniquePKval(FKValue): #For an FKTable, the FK in question is being used as the PK
            ## Yes (is unique)- It's not there yet so add a row with PK = FKValue, the keyValue as data to that row.
            try:
                # Store a record with the PK value (FK as PK) in one bin and start the list of parent keys in the next
                Table._CurrentClient.put(key, {self.PK[0]:FKValue, "TableHashes":[parent_keyValue]})
            except Exception as e:
                print("error: {0}".format(e), sys.stderr)
            self.Rows.add(hashkey)     # SortedList
        else:
            ##  No (not unique) so we need to add the parent_keyValue to an existing row.
            ## Add the parent_keyValue to the list for this PK
            try:
                (key, metadata, record) = Table._CurrentClient.get(key) #Do an RMW, adding the parent key to the list
                currentHashList = SortedList(record["TableHashes"]) # Use these 2 lines for Sorted
                currentHashList.add(parent_keyValue)
                #currentHashList = record["TableHashes"] # Use these 2 lines for unsorted
                #currentHashList.append(parent_keyValue)
                Table._CurrentClient.put(key, {"TableHashes":currentHashList})
            except Exception as e:
                print("error: {0}".format(e), sys.stderr)
    def Update(self, listOfValues):
        # This code is nearly identical to that of Insert function. See comments there for
        # basic understanding, comments here will be for changes due to Update vs Insert
        currentPKvals = []
        for PKcol in self.PK:
            currentPKvals.append(listOfValues[self.Columns.index(PKcol)])
        if Table._Debug: print("PKvals from Update: ", currentPKvals, "for Table: ", self.TableName)
        if Table._VerifyConstraints:
            if not self.VerifyStructure(listOfValues):
                print("Update not successful, VerifyStructure failed")
                return False
            # Note: Opposite logic as insert. Can't update a record that doesn't exist.
            #       By skipping this verification (VerifyConstraints = False) Update becomes an Insert
            if self.VerifyUniquePKval(currentPKvals): 
                print("Update failed! PK value doesn't currently exist in Table: ", self.TableName)
                return False
            #if Table has FKkeys, VerifyFKs (the updated values) exist in the refTable. Same as for Insert
            if len(self.FK) > 0:   
                currentFKvals = []
                for FKcol in self.FK:
                    currentFKvals.append(listOfValues[self.Columns.index(FKcol["colName"])])
                if Table._Debug: print("FKvals from Update: ", currentFKvals, "for Table: ", self.TableName)
                if not self.VerifyFKvalExists(currentFKvals):
                    print("Update failed! FK values don't exist in referenced Table!")
                    return False 
        #if all of the verifies are successful, update the data.
        #  -JSONize it (which means forming a dictionary by combining listOfValues with Columns names)
        string4JSON = dict(zip(self.Columns, listOfValues))
        jsonString = json.dumps(string4JSON)
        if Table._Debug: print("JSON string:", jsonString)
        hashkey = self.getHashKey(str(currentPKvals))
        key = (self.NameSpace, self.TableName, None, hashkey)
        try:
          # Write a record
          #client.put(key, jsonString)
          Table._CurrentClient.put(key, string4JSON)
        except Exception as e:
            print("error: {0}".format(e), sys.stderr)
        #No need to append a new hashkey since an update assumes we're not adding a new hashkey (just updating data)
        #self.Rows.add(hashkey)     # SortedList
        return True
    def Delete(self, listOfPKValues):
        if self.VerifyUniquePKval(listOfPKValues): # A True result implies the values don't yet exist in the table.
            print("Error: PK values specfied don't exist in ", self.TableName)
            return False
        #hashkey to remove from FKTable or to use for generating a key to remove a record
        parent_hashkey = self.getHashKey(listOfPKValues)
        # Not using FKTables. Just remove the record if it exists.
        if Table._UseFKTables == False:
            if Table._Debug: print("****Removing record ", listOfPKValues)
            key = (self.NameSpace, self.TableName, None, parent_hashkey)
            try:
                Table._CurrentClient.remove(key) #delete from the database
            except Exception as e:
                print("Error: Attempted delete. Not using FKTables")
                print("Error: {0}".format(e), sys.stderr)
            self.Rows.remove(parent_hashkey)
            return True
        #Implemente Restricted Delete. If the PK value is referenced by another table, the Delete will fail.
        ## When a row delete request is made at the parent Table, remove the row hash value from all child 
        #  FK value rows. If no more hashes exist in that row, the row can be deleted.
        ## When a row delete request is made at another table that is referenced by the parent table with 
        #  an FK, search for that FK’s value in the corresponding FKTable child table. If it exists, block 
        #  the delete. If it does not exist, complete the delete.

        #Case 1: Calling table has FKs in it.
        if len(self.FK) > 0:
            if Table._Debug: print("****This table has FK refs (it's a parent):", self.TableName)
            #Put listOfPKValues in a dict with their colNames so we can easily refer to them
            FKPKDict = dict(zip(self.PK, listOfPKValues)) #Some of these are not FKs but they won't be requested.
            for thisFKref in self.FK:
                FKTableToUpdate = self.FKtables[str(thisFKref['colName'] + '_FKTable')]
                #If the FK is part of the PK, it's already in this dict.
                if thisFKref['colName'] in self.PK:
                    checkFKvalue = FKPKDict[thisFKref['colName']]
                else: #If the FK isn't part of the PK, we'll need to loop up its value so we can find it in the FKTable.
                    record = self.Read_PK(listOfPKValues)
                    checkFKvalue = record[thisFKref['colName']]
                hashkey = FKTableToUpdate.getHashKey([checkFKvalue])
                key = (FKTableToUpdate.NameSpace, FKTableToUpdate.TableName, None, hashkey)
                if Table._Debug: print("****Updating FKtable ", FKTableToUpdate.TableName)
                try:
                    (key, metadata, record) = Table._CurrentClient.get(key) #Do an RMW, revmoing the parent key fromm the list
                    currentHashList = SortedList(record["TableHashes"]) 
                    if len(currentHashList) > 1:
                        currentHashList.remove(parent_hashkey)
                        Table._CurrentClient.put(key, {"TableHashes":currentHashList})
                    else:
                        Table._CurrentClient.remove(key) #delete from the database
                        FKTableToUpdate.Rows.remove(hashkey) #delete from the target table's list of record hashkeys.
                except Exception as e:
                    print("error: {0}".format(e), sys.stderr)
                    print("Error trying to delete in ", FKTableToUpdate.TableName)
                    return False
            #if all that went well, actually remove the requested record
            if Table._Debug: print("****Removing record ", listOfPKValues)
            key = (self.NameSpace, self.TableName, None, parent_hashkey)
            try:
                Table._CurrentClient.remove(key) #delete from the database
            except Exception as e:
                print("Error: Attempted delete. Using FKTables, this table has FKs")
                print("Error: {0}".format(e), sys.stderr)
            self.Rows.remove(parent_hashkey)
        else:
            #Case 2: Calling table has no FKs (but it might be referenced)
            if Table._Debug: print("****This table has no FK refs, checking for references:", self.TableName)
            #Need to determine if the values in the delete are used in a referencing table. If yes, terminate
            #the delete. Otherwise, let it continue.
            PKDict = dict(zip(self.PK, listOfPKValues))
            PKvalueIsReferenced = False
            for thisTable in Table._registry:
                for thisFKref in thisTable.FK: # If there are 0 elements in FK[], the table isn't ref'g the calling table
                    if thisFKref['refTable'] == self: # The calling table is refd by this table. Now look for this value
                        checkPKvalue = PKDict[thisFKref['refColName']]
                        FKTableToCheck = thisTable.FKtables[str(thisFKref['colName'] + '_FKTable')]
                        if FKTableToCheck.VerifyUniquePKval(checkPKvalue) == False: #If False, then the value exists in the FKTable
                            PKvalueIsReferenced = True
            if PKvalueIsReferenced:
                print("WARNING: Can't delete this record, it is referenced by another record!")
                return False
            else:
                if Table._Debug: print("****No references found, removing record", listOfPKValues)
                key = (self.NameSpace, self.TableName, None, parent_hashkey)
                try:
                    Table._CurrentClient.remove(key) #delete from the database
                except Exception as e:
                    print("Error: Attempted delete. Using FKTables, this table has no FKs")
                    print("Error: {0}".format(e), sys.stderr)
                self.Rows.remove(parent_hashkey)
        return True
    def Read_PK(self, PKValues):
        hashkey = self.getHashKey(str(PKValues))
        key = (self.NameSpace, self.TableName, None, hashkey)
        (key, metadata, record) = Table._CurrentClient.get(key)
        return record
    def Read_hashkey(self, hashkey):
        key = (self.NameSpace, self.TableName, None, hashkey)
        (key, metadata, record) = Table._CurrentClient.get(key)
        return record
    def getHashKey(self, PKValues):
        hashkey = Table._CurrentClient.get_key_digest(self.NameSpace, self.TableName, str(PKValues))
        return hashkey

