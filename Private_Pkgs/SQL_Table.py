#!/usr/bin/env python
# coding: utf-8
import sys
import json
from sortedcontainers import SortedList
import mysql.connector
from mysql.connector import errorcode

class Table:
    'Class that implements the same API framework as AS_Table.py, but for use with a MySQL DB'

    #Class variables   
    _registry = []
    _VerifyConstraints = True
    _UseFKTables = True
    _CurrentClient = None
    _sqlCursor = None
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
        print("*** SetVerifyConstraints function does nothing in SQL_Table")
        return
    @staticmethod
    def GetVerifyConstraints():
        print("*** GetVerifyConstraints function always returns True in SQL_Table")
        return True
    @staticmethod
    def UseFKTables(EnabledTF):
        print("*** UseFKTables function does nothing in SQL_Table")
        return
    @staticmethod
    def GetUseFKTables():
        print("*** GetUseFKTables function always returns True in SQL_Table")
        return True
    @staticmethod
    def SetTableClient(thisClient, thisCursor = None):
        Table._CurrentClient = thisClient
        if thisCursor != None:
            Table._sqlCursor = thisCursor
        print("Info: Table Client Set, connection state is: ", Table._CurrentClient.is_connected())
    @staticmethod
    def AddTableRowHash(TableName, hashkey):
        print("AddTableRowHash function not needed in SQL_Table")
        return True
    @staticmethod
    def RemoveAllTables(thisClient, Force = False):
        if Force == False:
            confirmation = input("Do you want to remove all tables? (yes to confirm): ")
            if confirmation != "yes":
                print("Note: Ignoring call to RemoveAllTables")
                return False
        for indexTable in reversed(Table._registry): #Get the table object reference
            # Note: Use Reversed on the assumption that this will avoid table to table FK constraint problems
            print("Removing SQL Table ", indexTable.TableName, "and all rows in the table")
            SQLstring = "DROP TABLE IF EXISTS "+ indexTable.TableName + ";"
            indexTable.sendToSQLDB(SQLstring, "Dropping Table")
        Table._registry.clear()
        return

    #constructor
    def __init__(self, namespace, name):
        self.NameSpace = namespace # For SQL, this means the database name.
        self.TableName = name
        self.Columns = [] # List of colNames for this Table.
        self.PK = [] # List of colNames that are to be used as PKs for this Table.
        self.FK = [] # List of dicts composed of {'colName': someColName, 'refTable': <obj ref>, 'refColName': otherColName}
        self.FKnames = [] # simplified list of FK colNames for this Table
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

    def VerifyFKrefsCompletePK(self):
        # This will be called to verify constraints when using Aerospike. When using MySQL, it will actually make 
        # the CREATE TABLE call to the database. Much different functions, but occur at the same point in the database
        # instantiation process (after columns, PKs, and FKs have been specified)
        createTableStr = "CREATE TABLE " + self.TableName + " ("
        for column in self.Columns:
            createTableStr = createTableStr + column + " VARCHAR(25), "
        # Start the constraint specification, PK(s) first
        createTableStr = createTableStr + "CONSTRAINT " + self.TableName + "_PK " + "PRIMARY KEY("
        for PKcolumn in self.PK:
            createTableStr = createTableStr + PKcolumn + ", "
        createTableStr = createTableStr[0:len(createTableStr)-2] + ")" #remove the extra ,<space> and close ()'s
        if len(self.FK) > 0:
            createTableStr = createTableStr + ", "
            FKconstNum = 1
            for FKcolumn in self.FK:
                createTableStr = createTableStr + "CONSTRAINT " + self.TableName + "_FK" + str(FKconstNum) + " FOREIGN KEY(" + FKcolumn["colName"] + ") "
                createTableStr = createTableStr + "REFERENCES " + FKcolumn["refTable"].TableName + "(" + FKcolumn["refColName"] + "), "
                FKconstNum += 1
            createTableStr = createTableStr[0:len(createTableStr)-2] #remove the extra ,<space> 
        createTableStr += ");" # and close ()'s
        success = self.sendToSQLDB(createTableStr, "Creating SQL table")
        return success
    def Report(self):
        print("Database Name: ", self.NameSpace)
        print("Table Name   : ", self.TableName)
        print("Columns      : ", self.Columns, "Total: ", len(self.Columns))
        print("PK Columns   : ", self.PK, "Total: ", len(self.PK))
        print("FK Columns   : ", self.FK, "Total: ", len(self.FK))
        print("FK Col Names : ", self.FKnames, "Total: ", len(self.FKnames))
        print("1st 3 Rows of ", len(self.Rows), " rows total:")
        for thisRow in self.Rows:
            if self.Rows.index(thisRow) >= 3: break
            print("Application storage ", thisRow)
    def Insert(self, listOfValues):
        insertTableStr = "INSERT INTO " + self.TableName + " VALUES(" + str(listOfValues)[1:-1] + ");"
        success = self.sendToSQLDB(insertTableStr, "Insert into SQL table")
        currentPKvals = []
        for PKcol in self.PK:
            currentPKvals.append(listOfValues[self.Columns.index(PKcol)])
        self.Rows.add(currentPKvals) # add because this is SortedList. AS would add hashkey, for SQL we'll store PKs
        return success
    def Update(self, listOfValues):
        # This code is nearly identical to that of Insert function. See comments there for
        # basic understanding, comments here will be for changes due to Update vs Insert
        # UPDATE schema1_a SET col2 = '222' WHERE col0 = 1;
        updateTableStr = "UPDATE " + self.TableName + " SET "
        for colName in self.Columns:
            updateTableStr += colName + " = '" + str(listOfValues[self.Columns.index(colName)]) + "', "
        updateTableStr = updateTableStr[0:len(updateTableStr)-2] + " WHERE " #remove the extra ,<space>, add WHERE
        for PKcol in self.PK:
            updateTableStr += PKcol + " = " + str(listOfValues[self.Columns.index(PKcol)]) + " AND "
        updateTableStr = updateTableStr[0:len(updateTableStr)-5] + ";" #remove the extra AND add ;
        success = self.sendToSQLDB(updateTableStr, "Update SQL table")
        return success
    def Delete(self, listOfPKValues):
        # DELETE FROM schema1_a WHERE col0 = 1;
        deleteTableStr = "DELETE FROM " + self.TableName + " WHERE "
        for PKcol in self.PK:
            deleteTableStr += PKcol + " = " + str(listOfPKValues[self.Columns.index(PKcol)]) + " AND "
        deleteTableStr = deleteTableStr[0:len(deleteTableStr)-5] + ";" #remove the extra , add ;
        success = self.sendToSQLDB(deleteTableStr, "Delete rows from SQL table")
        return success
    def sendToSQLDB(self, strToSend, possibleErrMsg):
        try:
            Table._sqlCursor.execute(strToSend)
        except mysql.connector.Error as err:
            print("*** SQL Error: ", possibleErrMsg, ", ", self.TableName, ", Error: ", err.msg)
            return False
        # No error, successful execute, so commit
        Table._CurrentClient.commit()
        return True
