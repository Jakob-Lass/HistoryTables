import unittest
import pytest
import HistoryTable
import warnings
warnings.simplefilter("ignore", ResourceWarning)

class HistoryTableTest(unittest.TestCase):
    def setUp(self):
        self.conn,self.cursor = HistoryTable.connect(host="localhost",
                                                     user="python",
                                                     passwd="python",
                                                     database = 'mydatabase')

        self.cursor.execute("SHOW TABLES")
        tables = [x[0] for x in self.cursor]
        if 'customers' in tables:
            self.cursor.execute("DROP TABLE customers")
        self.cursor.execute("CREATE TABLE customers (name VARCHAR(255),address VARCHAR(255))")
        self.conn.commit()
    
    
    def test_insert_data(self):
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        self.cursor.execute(sql, ('John', 'Highway 21'))
        
        self.cursor.execute('SELECT * FROM customers')
        Values = self.cursor.fetchall()
        if len(Values) == 0:
            self.fail("test_insert_data: Nothing written to database. Test Failed.")
        elif len(Values)>1:
            self.fail("test_insert_data: More than one entry in database. Test Failed. \nDatabase contents: \n{}".format([x for x in Values]))

    def test_remove_data(self):
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        self.cursor.execute(sql, ('John', 'Highway 21'))
        
        self.cursor.execute("DELETE FROM customers WHERE name = 'John'")
        
        self.cursor.execute('SELECT * FROM customers')
        Values = self.cursor.fetchall()
        if len(Values)!=0:
            self.fail('Database is non-empty despite expected to be empty.\nDatabase contents: \n{}'.format([x for x in Values]))

    def test_HistoryTable_contents(self):
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('John', 'Highway 21')
        self.cursor.execute(sql, val)
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('Janice', 'Highway 21')
        self.cursor.execute(sql, val)
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('Jack', 'Highway 22')
        self.cursor.execute(sql, val)

        names = ['John','Janice']

        self.cursor.execute('DELETE FROM customers WHERE address = "Highway 21"')
        self.cursor.execute('DELETE FROM customers WHERE address = "Highway 22"')

        self.cursor.execute('SELECT * FROM HISTORY')
        Values = self.cursor.fetchall()
        for id, action_id, table_id,action_name, action_values, action_conditions, action_time in Values:
            if id == 1:
                if not action_id == '1' and table_id == '1' and action_name == 'INSERT' and action_values == 'John, Highway 21' and \
                    action_conditions is None:
                    self.fail('First input of history table expected to be\n{}\nbut received{}'.format([1, 1, 1, 'INSERT', 'John, Highway 21', None],
                    id, action_id, table_id,action_name, action_values, action_conditions, action_time))
            if id in [4,5]: # deletion of multiple values
                if not action_id == '4' and table_id == '1' and action_name == 'DELETE' and action_values is None:
                    if not action_conditions == 'name = "{}" AND address = "Highway 21"'.format(names[id-4]):
                        self.fail('Deletion statement for multiple in history is wrong, expected:\n{}\nreceived:\n{}'.format((4, 1, 'DELETE', None, 'name = "John" AND address = "Highway 21"'),
                        (action_id, table_id,action_name, action_values, action_conditions)))
            if id == 6: # deletion of single value
                if not action_id == '5' and table_id == '1' and action_name == 'DELETE' and action_values is None and\
                    action_conditions == 'name = "Jack" AND address = "Highway 22"':
                    self.fail('Single delete statement wrong. Expected:\n{}\nReceived:\n{}'.format((5, 1, 'DELETE', None, 'name = "Jack" AND address = "Highway 22"'),
                    (action_id, table_id,action_name, action_values, action_conditions)))


        self.cursor.execute('SELECT * FROM HISTORYTABLES') # only one table is inserted
        tables = self.cursor.fetchall()
        if len(tables)>1:
            self.fail('Multiple tables found in HISTORYTABLES:\n{}',tables)
        elif len(tables) == 0:
            self.fail('No tables found in HISTORYTABLES, expected 1')

        table_id, table_name, table_attributes = tables[0]
        if not table_id == 1 and table_name == 'customers' and table_attributes == 'name, address':
            self.fail('Wrong values for table in HISTORYTABLES. Expected:\n{}\nReceived:\n{}'.format((1, 'customers', 'name, address'),(table_id, table_name, table_attributes)))




    def test_Reversion_insert(self):
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('John', 'Highway 21')
        self.cursor.execute(sql, val)
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('Janice', 'Highway 21')
        self.cursor.execute(sql, val)
        HistoryTable.revertID(self.cursor,1)
        self.cursor.execute('SELECT * FROM customers') # only one table is inserted
        customers = self.cursor.fetchall()
        if not len(customers) == 1:
            self.fail('Number of customers is wrong. Got {} but expected 1'.format(len(customers)))
        
        customer = customers[0]
        if not customer == val:
            self.fail('Values of customer is wrong. Expected:\n{}\nReceived:\n{}'.format(val,customer))

    def test_Reversion_delete(self):
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('John', 'Highway 21')
        self.cursor.execute(sql, val)
        sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
        val = ('Janice', 'Highway 21')
        self.cursor.execute(sql, val)
        self.cursor.execute('DELETE FROM customers WHERE name = "John" AND address = "Highway 21"') # Full specification
        self.cursor.execute('DELETE FROM customers WHERE name = "Janice"') # Single specification

        HistoryTable.revertID(self.cursor,4)
        HistoryTable.revertID(self.cursor,3)

        self.cursor.execute('SELECT * FROM customers') # only one table is inserted
        customers = self.cursor.fetchall()
        if not customers == [('Janice', 'Highway 21'),('John', 'Highway 21')]:
            self.fail('Reversion of deletion failed. Expected to find:\n{}\nin customers but got:\n{}'.format([('Janice', 'Highway 21'),('John', 'Highway 21')],customers))

        
        

    def tearDown(self):
        self.cursor.execute('DROP TABLE HISTORY')
        self.cursor.execute('DROP TABLE HISTORYTABLES')
        self.cursor.execute("DROP TABLE customers")
        self.conn.commit()
        self.conn.close()

if __name__ == "__main__":
    unittest.main(warnings='ignore')