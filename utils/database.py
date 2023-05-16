import sqlite3

from sqlite3 import Error


class Database:

    def __init__(self, database_name):
        try:
            self.name = database_name + '.db'

            self.con = sqlite3.connect(self.name)

        except Error:

            print(Error)


    def get_con(self):
        return self.con

    def create_table(self, tablename, columns):

        cursorObj = self.con.cursor()

        cursorObj.execute(f"CREATE TABLE if not exists {tablename}({columns})")

        self.con.commit()

    def fetch_all_table_name(self):

        cursorObj = self.con.cursor()

        cursorObj.execute(f'SELECT name from sqlite_master where type= "table"')

        print(cursorObj.fetchall())

    def insert_data_to_table(self, tablename, columns, values):
        cursorObj = self.con.cursor()
        try:
            cursorObj.execute(
                f'INSERT INTO {tablename}({columns}) VALUES({values})')
        except sqlite3.IntegrityError as dbinsertexception:
            print(dbinsertexception)
        self.con.commit()

    def fetch_all(self, column, tablename):
        cursorObj = self.con.cursor()

        cursorObj.execute(f'SELECT {column} FROM {tablename}')

        rows = cursorObj.fetchall()

        # for row in rows:
        #     print(row)
        return rows

    def update(self, table, data, column, comparator, value):
        cursorObj = self.con.cursor()
        cursorObj.execute(f'UPDATE {table} SET {data} WHERE {column} {comparator} {value}')
        self.con.commit()

    def delete_table(self, tablename):
        cursorObj = self.con.cursor()
        cursorObj.execute(f'DROP table if exists {tablename}')
        self.con.commit()

    def rowcount(self, column, tablename):
        cursorObj = self.con.cursor()

        cursorObj.execute(f'SELECT {column} FROM {tablename}')

        rows = cursorObj.fetchall()

        print(len(rows))

    def custom_sql_query(self, sql_query):
        cursorObj = self.con.cursor()

        cursorObj.execute(sql_query)
        # print(cursorObj.fetchall())
        self.con.commit()
        return cursorObj.fetchall()
