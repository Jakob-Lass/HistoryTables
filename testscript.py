#!/usr/local/bin/python3 
"""
  Testing script
"""

import mysql.connector
import numpy as np


mydb = mysql.connector.connect(
    host="localhost",
    user="python",
    passwd="python",
    database = 'mydatabase'
)

logActions = ['CREATE','INSERT','DELETE','ALTER']

mycursor = mydb.cursor()

mycursor.execute("DROP TABLE customers")
mycursor.execute("DROP TABLE HISTORY")
mycursor.execute("DROP TABLE HISTORYTABLES")


mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")
mycursor.execute("SHOW TABLES")
tables = [x[0] for x in mycursor]


if not "HISTORY" in tables:
    mycursor.execute("CREATE TABLE HISTORY (id INT AUTO_INCREMENT PRIMARY KEY, table_id INT, "
    "action_name VARCHAR(255), action_values VARCHAR(255), action_conditions VARCHAR(255), action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

if not "HISTORYTABLES" in tables:
    mycursor.execute("CREATE TABLE HISTORYTABLES (id INT AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(255), "
    "table_attributes VARCHAR(255))")
    

def extractAction(call):
    return call.split(' ')[0]

def extractValues(call,values):
    if not values is None:
        #if np.any(["'" == x[0] for x in values]) # Test if string arguments use ' or "
        return ', '.join(values)
    
    if "VALUES" in call:
        idx = call.index('VALUES')+len('VALUES')
        return call[idx:]
    else:
        return None

def extractCondition(call,value):
    call = call.format(value)
    if 'WHERE' in call:
        idx = call.index('WHERE')
        return call[idx+len('WHERE'):]
    else:
        return None
        

def extractTable(call):
    splitted = call.split(' ')
    if 'INTO' in splitted:
        idx = splitted.index('INTO')+1
    elif 'FROM' in splitted:
        idx = splitted.index('FROM')+1
    elif 'TABLE' in splitted:
        idx = splitted.index('TABLE')+1
    else:
        idx = 2
    return splitted[idx]
    
def revertCall(table_name,action_name,action_values,action_conditions,table_columns):
    revertCall = []
    
    if action_name == 'INSERT':
        
        revertCall = 'DELETE from {} WHERE '.format(table_name)
        
        testClause = ' AND '.join(["{} = '{}'".format(column.strip(),value.strip()) for column,value in zip(table_columns.split(','),action_values.split(','))])
        revertCall = ''.join([revertCall,testClause])
        return revertCall
    if action_name == 'DELETE':
        revertCall = 'INSERT INTO {} '.format(table_name)
        pairs = action_conditions.split(' AND ')
        fields = []
        values = []
        for pair in pairs:
            field, value = [x.strip() for x in pair.split(' = ')]
            fields.append(field.replace("'",'').replace('"',''))
            values.append(value)
        #print(['(',*[', '.join([x for x in fields])],')'])
        fieldCall = ''.join(['(',*[', '.join([x for x in fields])],')'])
        valueCall = ''.join(['(',*[', '.join([x for x in values])],')'])
        revertCall = ''.join([revertCall,fieldCall,' VALUES ',valueCall])
        #sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        #val = ('John', 'Highway 21')
        print(revertCall)
        return revertCall
    else:
        raise AttributeError('Command "{}" not understood... sorry :-/'.format(action_name))
    
def revertID(mycursor,checkId):
    mycursor.execute("SELECT * FROM HISTORY WHERE HISTORY.id = {} ".format(checkId))
    id, table_id, action_name, action_values, action_conditions, action_time  = mycursor.fetchone()
    mycursor.execute("SELECT table_name,table_attributes FROM HISTORYTABLES WHERE id = {} ".format(table_id))
    table_name,table_columns = mycursor.fetchone()

    mycursor.execute(revertCall(table_name,action_name,action_values,action_conditions,table_columns),log=False)
    

def decorator(self,execute):
    
    def func(call,values=None,log=True):
        #call = call.format(values)
        action = extractAction(call)
        
        if action in logActions and log == True:
            table = extractTable(call)
            
            conditions = extractCondition(call,values)
            vals = extractValues(call,values)  
            
            execute("SELECT id from HISTORYTABLES WHERE table_name = '{}'".format(table))
            tableID = self.fetchone()
            
            if tableID is None:
                #print('Table "{}" not found.'.format(table))
                execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(table))
                attributes = ', '.join([str(x[0]) for x in self.fetchall()])
                #print('Attributes:',attributes)
                execute("INSERT INTO HISTORYTABLES (table_name,table_attributes) VALUES ('{}','{}')".format(table,attributes))
                tableID = self.lastrowid
                #print('Inserting new with id {}'.format(tableID))
            else:
                tableID = tableID[0]
            
            if action == 'DELETE':
                #print('Running:',"SELECT * from {} WHERE {}".format(table,conditions))
                execute("SELECT * from {} WHERE {}".format(table,conditions))
                vals = ', '.join(self.fetchone())
                execute('SELECT table_attributes from HISTORYTABLES WHERE id = "{}"'.format(tableID))
                attributes = self.fetchone()
                print(attributes,vals)
                conditions = ' AND '.join(['{} = "{}"'.format(a,v) for a,v in zip(attributes[0].split(', '),vals.split(', '))])
                print(conditions)
                vals = None


            historyCall = 'INSERT INTO HISTORY (table_id, action_name, action_values, action_conditions) VALUES (%s, %s, %s, %s)'
            execute(historyCall,(tableID,action,vals,conditions))
            _ = self.lastrowid

            
        return execute(call,values)
    return func
    

#mycursor.execute("SHOW TABLES")
#tables = [x[0] for x in mycursor]

#print('Pre working:')
#print('Following tables exist',tables)
#for table in tables:
#    print('Contents of {}'.format(table))
#    mycursor.execute('SELECT * FROM {}'.format(table))
#    [print(x) for x in mycursor]
#    print('')


mycursor.execute = decorator(mycursor,mycursor.execute)


sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
val = ('John', 'Highway 21')
mycursor.execute(sql, val)

sql = "DELETE from customers WHERE address = 'Highway 21'"

mycursor.execute(sql)

mycursor.execute("SHOW TABLES")
tables = [x[0] for x in mycursor]
#print('\n\nDuring working:')
#print('Following tables exist',tables)

#for table in tables:
#    print('Contents of {}'.format(table))
#    mycursor.execute('SELECT * FROM {}'.format(table))
#    [print(x) for x in mycursor]
#print('\n\n')

checkId = 2

revertID(mycursor,checkId)

checkId = 1
revertID(mycursor,checkId)

for table in tables:
    print('Contents of {}'.format(table))
    mycursor.execute('SELECT * FROM {}'.format(table))
    [print(x) for x in mycursor]
print('\n\n')



#revertCall = 'DELETE from TABLE {} WHERE '.format(table_name)
#testClause = ' AND '.join(['{} = {}'.format(column.strip(),value.strip()) for column,value in zip(table_columns.split(','),action_values.split(','))])


#mydb.commit() # To commit actual changes

# List of calls:
# CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))
# ALTER TABLE customers ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY
# INSERT INTO customers (name, address) VALUES (%s, %s)