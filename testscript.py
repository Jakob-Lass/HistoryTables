#!/usr/local/bin/python3 
"""
  Testing script
"""

import HistoryTable
import numpy as np


connection,myCursor = HistoryTable.connect(
    host="localhost",
    user="python",
    passwd="python",
    database = 'mydatabase'
)

myCursor.execute("SHOW TABLES")
tables = [x[0] for x in myCursor]

if 'customers' in tables:
    myCursor.execute("DROP TABLE customers")



myCursor.execute("CREATE TABLE customers (name VARCHAR(255),address VARCHAR(255))")

HistoryTable.printTables(myCursor)

sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"


myCursor.execute(sql, ('John', 'Highway 21'))

if True:
    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ('Jane', 'Highway 21')
    myCursor.execute(sql, val)
    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ('Janice', 'Highway 21')
    myCursor.execute(sql, val)
    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ('Jack', 'Highway 22')
    myCursor.execute(sql, val)
    #HistoryTable.printTables(mycursor)


sql = "DELETE from customers WHERE address = 'Highway 21'"

myCursor.execute(sql)


HistoryTable.printTables(myCursor)

checkId = 1
HistoryTable.revertID(myCursor,checkId)

checkId = 5
HistoryTable.revertID(myCursor,checkId)

#checkId = 4
#HistoryTable.revertID(myCursor,checkId)


HistoryTable.printTables(myCursor)

connection.commit() # To commit actual changes

connection.close()
# List of calls:
# CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))
# ALTER TABLE customers ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY
# INSERT INTO customers (name, address) VALUES (%s, %s)

