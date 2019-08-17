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

if 'customers' in tables:
    mycursor.execute("DROP TABLE customers")
mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")


sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
val = ('John', 'Highway 21')
mycursor.execute(sql, val)

if True:
    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ('Jane', 'Highway 21')
    mycursor.execute(sql, val)
#HistoryTable.printTables(mycursor)


sql = "DELETE from customers WHERE address = 'Highway 21'"

mycursor.execute(sql)



checkId = 3

HistoryTable.revertID(mycursor,checkId)

checkId = 4
HistoryTable.revertID(mycursor,checkId)


HistoryTable.printTables(mycursor)



#revertCall = 'DELETE from TABLE {} WHERE '.format(table_name)
#testClause = ' AND '.join(['{} = {}'.format(column.strip(),value.strip()) for column,value in zip(table_columns.split(','),action_values.split(','))])


#mydb.commit() # To commit actual changes

# List of calls:
# CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))
# ALTER TABLE customers ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY
# INSERT INTO customers (name, address) VALUES (%s, %s)