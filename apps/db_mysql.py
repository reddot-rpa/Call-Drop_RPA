import mysql.connector
from mysql.connector import Error


class MySQLDatabase:
    def __init__(self):
        self.host = '10.101.72.12'
        self.database = 'wf_robiworkflow'
        self.port = 33060
        self.username = 'processmaker'
        self.password = '9$0bU8ZPmR0B!48'
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(host=self.host,
                                                      database=self.database,
                                                      port=self.port,
                                                      user=self.username,
                                                      password=self.password)

            if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = self.connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)
                cursor.close()

        except Error as e:
            print("Error while connecting to MySQL", e)

    def create_table(self, table_query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(table_query)
            print('Table created successfully')
            cursor.close()
        except Error as e:
            print("Failed to create table in MySQL: {}".format(e))

    def insert_data_into_table(self, insert_query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(insert_query)
            self.connection.commit()
            print("Record inserted successfully")
            cursor.close()
        except Error as e:
            print("Failed to insert record {}".format(e))

    def select_data_from_table(self, select_query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(select_query)
            records = cursor.fetchall()
            return records
        except Error as e:
            print("Error reading data from MySQL table", e)

    def update_data_from_table(self, update_query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(update_query)
            self.connection.commit()
            print("Record Updated successfully")
            cursor.close()
        except Error as e:
            print("Failed to update table record: {}".format(e))

    def delete_data_from_table(self, delete_query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(delete_query)
            self.connection.commit()
            print("Record Deleted successfully ")
            cursor.close()
        except Error as e:
            print("Failed to delete record from table: {}".format(e))
