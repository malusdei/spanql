#!/bin/python
import sys
import re
import datetime
import pprint
import random
from google.cloud import spanner
from google.cloud.spanner_v1.proto import type_pb2


class clsSpanQL(object):
    # Receives SQL statements (which might not normally be compatible with Spanner) and turn them
    # into operations that Spanner can process.
    spannerInstance = ""
    spannerDB = ""
    strDB = ""
    strLogFile = ""
    strLogPos = ""


    def __init__(self, inst, db):
        # Later remember to issue fatal error if no Current DB
        self.spannerInstance = inst
        self.spannerDB = db

    def fnChangeDB(self, strSQL):
        self.strCurrentDB = strSQL

    def fnAbsSelect(self, sqlSelect):
        # Normally, a SELECT will not be alone in a replication event, but it will be a sub statement for an
        # INSERT, UPDATE, or DELETE, which can only operate by keys.
        # This function performs an actual SELECT, and then returns the PK values of the table.
        print sqlSelect

    def lsStrToList(self, strValues):
        # Need to split by comma, yet ignore commas in quotes
        lsNumChars = ['1','2','3','4','5','6','7','8','9','0','-','.']
        lsStrToList = []
        lsTemp = []
        #String, Int, or Float
        strType = ""
        i = 0
        while (i < len(strValues)):
            if (strValues[i] == '"'):
                strType = "text"
                j = strValues.find('"', i + 1)
                # Be super careful of escaped Quotes in Quotes
                while (strValues[j-1] == "\\"):
                    j = strValues.find('"', j + 1)
                # Doublecheck that it was properly terminated
                if (j == -1):
                    print "Double quote not properly terminated."
                    sys.exit()
                lsTemp.append(strValues[i+1:j])
                print ("Looking for end double quote")
                i = j + 1
            elif (strValues[i] == "'"):
                strType = "text"
                j = strValues.find("'", i + 1)
                # Be super careful of escaped Quotes in Quotes
                while (strValues[j-1] == "\\"):
                    j = strValues.find("'", j + 1)
                    print ("Looking for end single quote")
                # Doublecheck that it was properly terminated
                if (j == -1):
                    print "Single quote not properly terminated."
                    sys.exit()
                lsTemp.append(strValues[i+1:j])
                print ("Appending %s"%strValues[i+1:j])
                i = j + 1
            elif (strValues[i] == "("):
                # Start of list
                lsTemp = []
                print ("Start of List")
                i += 1
            elif (strValues[i] == ")"):
                # End of list
                lsStrToList.append(lsTemp)
                print ("End of List")
                print (lsTemp)
                i += 1
            elif (strValues[i] == ","):
                # End of field, start a new one
                print ("End of Field")
                i += 1
            elif (strValues[i] == " "):
                print ("Space, skipping")
                i += 1
            elif (strValues[i] in lsNumChars):
                j = i + 1
                while (strValues[j] in lsNumChars):
                    j += 1
                strTemp = strValues[i:j]
                if (strTemp.find(".") == -1):
                    strType = "int"
                    lsTemp.append(int(strTemp))
                else:
                    strType = "float"
                    lsTemp.append(float(strTemp))
                print ("Final Number %s."%strTemp)
                i = j
            else:
                # That should be all the cases.  If code lands here, something is wrong
                # Throw an exception and exit.
                print "Fatal error: unexpected character %s at position %i in: \n %s"%(strValues[i], i, strValues)
        return lsStrToList
    
    def lsFromSelect(strSelect):
        lsFromSelect = []
        with self.spannerDB.snapshot() as snapshot:
            result = snapshot.execute_sql(strSelect)
            for r in result:
                lsFromSelect.append(r)
        # Some caveats: a JOIN involving another schema will crash this Select statement.
        # It would be in our best interest to see if this might be the case and throw an
        # exception if it is.
        
    
    def fnAbsInsert(self, sqlInsert):
        lsDBTable = []
        lsData = []
        lsFields = []
        flgFieldsListed = 0
        lsValues = ()
        strValues = ""
        strData = ""
        sqlInsert = re.sub('\n', ' ', sqlInsert)
        sqlInsert = re.sub('\t', ' ', sqlInsert)
        sqlInsert = re.sub(' +', ' ', sqlInsert)
        lsInsert = sqlInsert.split(" ", 3)
        strTable = lsInsert[2]
        if (strTable.find(".") != -1):
            lsDBtable = strTable.split(".")
            strTable = lsDBTable[1]
            if (lsDBTable[0] == self.strDB):
                #Stop.  Throw an error.
                print "References table in another schema."
                sys.exit(1)
        # Get Fields
        if (lsInsert[3][0] == "("):
            strFields = re.sub(' ', '', lsInsert[3][1:lsInsert[3].find(")")])
            lsFields = strFields.split(",")
            flgFieldsListed = 1
        else:
            # Going to have to examine the table and get the fields names, in order
            # lsValues = lsGetFieldsFromTable
            flgFieldsListed = 0
        strData = lsInsert[3][lsInsert[3].find(")") + 1:].strip()
        # strData now either starts with "values" or "select"
        if (strData[0:6].lower() == "values"):
            lsData = self.lsStrToList(strData[6:].strip())
        
        if (strData[0:5].lower() == "select"):
            # Then it is a valid SELECT statement. We can run it against our own engine and enter the results ourselves.
            lsData = lsFromSelect(strData.strip())
            
        print (lsFields)
        print (lsData)
        """with self.db.batch() as batch:
            batch.insert(
                table=strTable,
                columns=tuple(lsFields),
                values=tuple(lsData))
        # We may need to force to those aforementioned Unicode values first.
        """

    def dictUpdateSet(strSet):
        # strSet is just the Set portion of an UPDATE.
        # It might also include a WHERE.
        lsNumChars = ['1','2','3','4','5','6','7','8','9','0','-','.']
        dictUpdateSet = {}
        dictTemp = {}
        tmpKey = ""
        tmpVal =""
        #String, Int, or Float
        strType = ""
        i = 4
        # Writing my own parser, to preclude special characters that might be escaped.
        tmpKey = strSet[4:].split("=")[0].strip()
        i = strSet.find("=") + 1
        while (i < len(strSet)):
            lsTemp = []
            if (strSet[i] == '"'):
                j = strSet.find('"', i + 1)
                # Be super careful of escaped Quotes in Quotes
                while (strSet[j-1] != "\\"):
                    j = strSet.find('"', j + 1)
                # Doublecheck that it was properly terminated
                if (j == -1):
                    print "Double quote not properly terminated."
                    sys.exit()
                dictUpdateSet[tempKey] = strSet[i+1:j]
                tempKey = ""
                i = j + 1
            elif (strSet[i] == "'"):
                strType = "text"
                j = strSet.find("'", i + 1)
                # Be super careful of escaped Quotes in Quotes
                while (strSet[j-1] != "\\"):
                    j = strSet.find("'", j + 1)
                # Doublecheck that it was properly terminated
                if (j == -1):
                    print "Single quote not properly terminated."
                    sys.exit()
                dictUpdateSet[tempKey] = strSet[i+1:j]
                tempKey = ""
                i = j + 1
            elif (strSet[i] in lsNumChars):
                j = i + 1
                while (strSet[j] in lsNumChars):
                    j += 1
                strTemp = strSet[i:j]
                if (strTemp.find(".") == -1):
                    dictUpdateSet[tempKey] = int(strTemp)
                    tempKey = ""
                else:
                    dictUpdateSet[tempKey] = float(strTemp)
                    tempKey = ""
                i = j
            elif (strSet[i] == ","):
                # End of field, start a new one
                i += 1
                tmpKey = strSet[i:].split("=")[0].strip()
                i += len(tmpKey)
            elif (strSet[i] == " "):
                i += 1
            elif (strSet[i:i+5].lower() == "where"):
                break
            else:
                # That should be all the cases.  If code lands here, something is wrong
                # Throw an exception and exit.
                # A "self-referencing" value, like UPDATE my_table SET my_field = my_field + 1
                # might not be able to be handled by Spanner.  Double check.
                print "Fatal error: unexpected character %s at position %i in: \n %s"%(strSet[i], i, strSet)
                
        # I could use some advice here. I need to return two things - the dictionary and an integer showing
        # the position of the syntactical WHERE clause.  I believe I will "cheat" by adding an additional
        # key that I *hope* no one will ever use, and then delete the key after the calling function is done.
        dictUpdateSet['dictUpdateSet'] = i
        return dictUpdateSet
    
    def lsPrimesBySelect(strTable, strWhere):
        # Spanner only allows UPDATE and DELETES by keys.  So, get the Primary Keys of a table.
        # Future task: need to allow querying for joined tables.
        dictPrimesBySelect = {}
        lsPKFields = []
        dictFieldVals = {}
        # Get key fields from information_schema
        with database.snapshot() as snapshot:
             results = snapshot.execute_sql("SELECT column_name \
                 FROM information_schema.index_columns \
                 WHERE table_name = '%s' AND index_type='PRIMARY_KEY' \
                 ORDER BY ordinal_position"%(strTable))
        for row in results:
            lsPKFields.append(row[0])
        # Select pk fields based on the WHERE, which had better be ONE record, and retrun field(s) and value(s)
        # Get key values with the strWhere criteria
        with database.snapshot() as snapshot:
             results = snapshot.execute_sql("SELECT %s \
                 FROM %s \
                 %s"%(lsPKFields.join(), strTable, strWhere))
        # Convert row to dictionary.
        # There are as many row elements as PKField elements
        for row in results:
            p = 0
            while (p < len(lsPKFields)):
                dictFieldVals[lsPKFields[p]] = row[p]
                p += 1
            lsPrimesBySelect.append(dictFieldVals)
        return lsPrimesBySelect


    def fnAbsUpdate(self, strTable, arrFields, arrValues, idKey):
        # Cloud Spanner can only work on the PK in UPDATE...WHERE statements.
        # Our compatilibility layer will simply fetch the keys first, and then
        # UPDATE based on the keys that came back.
        # (There is the unfortunate memory limit, which we won't know yet.)
        # [{key1:row1val1, key2:row1val2,...}, {key1:row2val1, key2:row2val2,...},...}
        sqlUpdate = re.sub('\n', ' ', sqlUpdate)
        sqlUpdate = re.sub('\t', ' ', sqlUpdate)
        sqlUpdate = re.sub(' +', ' ', sqlUpdate)
        lsValues = []
        lsUpdate = sqlUpdate.split(" ", 4)
        # [0] UPDATE
        # [1] Table Name
        # [2] SET...
        strSet = lsUpdate[1]
        dictP = {}
        dictUpdate = self.dictUpdateSet(lsUpdate[2])
        intWhere = dictUpdate['dictUpdateSet']
        # We do not need the key showing up as a field.
        del dictUpdate['dictUpdateSet']
        strWhere = ""
        if (strSet[strWhere:strWhere+5].lower() == "where"):
            strWhere = strSet[strWhere:].strip()
        # lsd = list of dictionaries
        lsdRecords = lsPrimesBySelect(strTable, strWhere)
        # Set/Replace keys from lsdRecords into dictUpdate
        for dRecord in lsdRecords:
            dictUpdate.update(dRecord)
            lsColumns = []
            lsTempVal = []
            for k in dictUpdate:
                lsColumns.append(k)
                lsTempVal.append(dictUpdate[k])
            lsValues.append(lsTempVal)
            with database.transaction() as transaction:
                transaction.update(
                    table=strTable,
                    columns=lsColumns,
                    values=lsValues)
        print (lsColumns)
        print (lsValues)
            
        
    def fnAbsDelete(self, strTable, idKey):
        # Like the UPDATE, CS can only work on the PK in DELETE...WHERE statements.
        # Our compatilibility layer will simply fetch the keys first, and then
        # DELETE based on the keys that came back.
        # (There is the unfortunate memory limit, which we won't know yet.)
        sqlUpdate = re.sub('\n', ' ', sqlUpdate)
        sqlUpdate = re.sub('\t', ' ', sqlUpdate)
        sqlUpdate = re.sub(' +', ' ', sqlUpdate)
        lsUpdate = sqlUpdate.split(" ", 3)
        # [0] DELETE
        # [1] Table Name
        # [2] WHERE
        

    def fnCreateTable(self, strTable, lsFields):
        print lsFields

    def fnAlterTable(self, strSQL):
        print strSQL
        
    def fnDropTable(self, strTable):
        print strTable


def main():
    # Instantiate a client.
    spanner_client = spanner.Client()

    # Your Cloud Spanner instance ID.
    instance_id = 'solomoninstance'

    # Get a Cloud Spanner instance by ID.
    instance = spanner_client.instance(instance_id)

    # Your Cloud Spanner database ID.
    database_id = 'bankdb'

    # Get a Cloud Spanner database by ID.
    database = instance.database(database_id)

    spank = clsSpanQL(instance, database)
    spank.fnAbsInsert("INSERT INTO tblMyTable (mt_id, mtval, fname, whatever) values (321, 111, 'Hey\\'s there', 'Balah Balah Balah!'), (234, 432, 'Ima Rockstar', 'Get Paid')")

if __name__ == "__main__":
    main()
