#!/usr/local/bin/python3 
"""
  Testing script
"""

import HistoryTable
import numpy as np


connection,mycursor = HistoryTable.connect(
    host="localhost",
    user="python",
    passwd="python",
    database = 'mydatabase'
)

mycursor.execute("SHOW TABLES")
tables = [x[0] for x in mycursor]

#for table in tables:
#    mycursor.execute("DROP TABLE {}".format(table))

def generateChar(N):
    integers = np.random.randint(0,51,size=(N))
    integers[integers>25]+=6
    return ''.join([chr(x) for x in integers+65])




tableName = generateChar(np.random.randint(20))

print('Random table is',tableName)
if tableName in tables:
    mycursor.execute("DROP TABLE {}".format(tableName))

attributesList = ['name','address']
attributes = ', '.join(['{} VARCHAR(255)'.format(att) for att in attributesList])
print("CREATE TABLE {} ({})".format(tableName,attributes))

mycursor.execute("CREATE TABLE {} ({})".format(tableName,attributes))



HistoryTable.printTables(mycursor)

try:
    sql = "INSERT INTO {} ({}) VALUES ({})".format(tableName,', '.join(attributesList),', '.join(['%s']*len(attributesList)))
    print('Calling:',sql)
    val = [generateChar(np.random.randint(20)) for _ in range(len(attributesList))]
    print('Values:',val)
    
    mycursor.execute(sql, val)

    if False:
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('Jane', 'Highway 21')
        mycursor.execute(sql, val)
        #HistoryTable.printTables(mycursor)


        sql = "DELETE from customers WHERE address = 'Highway 21'"

        mycursor.execute(sql)


    HistoryTable.printTables(mycursor)
    checkId = 1

    HistoryTable.revertID(mycursor,checkId)


    #checkId = 4
    #HistoryTable.revertID(mycursor,checkId)


    HistoryTable.printTables(mycursor)

except:

#revertCall = 'DELETE from TABLE {} WHERE '.format(table_name)
#testClause = ' AND '.join(['{} = {}'.format(column.strip(),value.strip()) for column,value in zip(table_columns.split(','),action_values.split(','))])


#mydb.commit() # To commit actual changes

# List of calls:
# CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))
# ALTER TABLE customers ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY
# INSERT INTO customers (name, address) VALUES (%s, %s)


# clean up

    mycursor.execute("DROP TABLE {}".format(tableName))
    raise 

else:
    mycursor.execute("DROP TABLE {}".format(tableName))