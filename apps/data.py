import sqlite3



conn = sqlite3.connect('custom.db')



#
# def create_connection():
#     """ create a database connection to a database that resides
#         in the memory
#     """
#     conn = None
#     # try:
#     conn = sqlite3.connect()
#     # sqlite3.co
#     print(sqlite3.version)
#     # except sqlite3.Error as e:
#     #     print(e)
#     # finally:
#     #     if conn:
#     #         conn.close()
#
#
# if __name__ == '__main__':
#     create_connection()
