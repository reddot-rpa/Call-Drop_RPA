from utils.logger import Logger
import cx_Oracle
import shutil
from apps.config import ConfigParser

log = Logger.get_instance()


class CallDropDB:
    db_host = ""
    db_port = ""
    db_service_name = ""
    db_user = ""
    db_password = ""

    def __init__(self):
        try:
            # print("Loading Database Configuration Files....")
            log.log_info("Loading Database Configuration Files....")
            self.conf = ConfigParser().conf
            self.db_host = self.conf['dbHost']
            self.db_port = self.conf['dbPort']
            self.db_service_name = self.conf['dbServiceName']
            self.db_user = self.conf['dbUser']
            self.db_password = self.conf['dbPassword']
            # print(self.db_password)
            self.crm_download_file_directory = self.conf['crm_download_file_directory']

            self._conn = None
            self._cursor = None
        except Exception as e:
            print(e)
            log.log_critical("Config file not loaded....")

    def connect(self):
        try:
            # print("Attempting ORCL Connection")
            log.log_info("Attempting ORCL Connection")
            dsn_tns = cx_Oracle.makedsn(self.db_host, self.db_port, service_name=self.db_service_name)
            self._conn = cx_Oracle.connect(user=self.db_user, password=self.db_password, dsn=dsn_tns)
            self._cursor = self._conn.cursor()
            print("Connected To Database")
        except Exception as e:
            log.log_critical(f"There is a problem with Oracle DETAIL(single_data_record): EXCEPTION - {e}")
            log.log_error(e)
            print(e)
            raise

    def close_connection(self):
        print("Closing connection")
        try:
            self._cursor.close()
            self._conn.close()
        except Exception as e:
            print(e)
            log.log_error(e)
            raise

    def commit_execution(self):
        self._conn.commit()

    def total_rows_count(self, query):
        log.log_info("Initiated totalRowsCount Query = " + query)
        return self.select_query(query, 'count')

    def select_single_row(self, query):
        log.log_info("Initiated singlerow Query = " + query)
        return self.select_query(query, 'singlerow')

    def select_query(self, query, return_type='row'):
        try:
            # connect to database
            self.connect()
            log.log_info("Initiated Query = " + query)

            if return_type == 'row':
                self._cursor.execute(query)
                res = self._cursor.fetchall()
            elif return_type == 'singlerow':
                self._cursor.arraysize = 1
                self._cursor.execute(query)
                res = self._cursor.fetchone()
            else:
                self._cursor.execute(query)
                self._cursor.fetchall()
                res = self._cursor.rowcount
            print('res = ', res)
            # closing database connection
            self.close_connection()
            if not res:
                log.log_critical("Query Failed")
                return 0
            else:
                log.log_info("Query Successful")
                return res
        except cx_Oracle.DatabaseError as e:
            print(e)
            self.close_connection()
            log.log_critical("Query Failed Due To " + str(e))
            return 0

    def execute_query(self, query):
        log.log_info("Attempting Query = " + query)

        try:
            # connect to database
            # self.connect()
            print("Attempting query = " + query)
            self._cursor.execute(query)
            self.commit_execution()
            log.log_info("Execution Successful")
            # print("Execution Successful")
            # closing database connection
            # self.close_connection()
            return 1
        except cx_Oracle.DatabaseError as e:
            log.log_critical("Execution Failed Due To " + str(e))
            #   self.close_connection()
            print(e)
            return 0

    def execute_many_query(self, query, param):
        print(f"Attempting Query = {query}")
        log.log_info(f"Attempting Query = {query}")
        try:
            # connect to database
            self.connect()
            # param = [
            #     (10, 'Parent 10'),
            #     (20, 'Parent 20'),
            #     (30, 'Parent 30'),
            #     (40, 'Parent 40'),
            #     (50, 'Parent 50')
            # ]
            # Query = "insert into RPA_DIV_ERASE_VOUCHER (COMPLAIN_ID, MSISDN) values (:1, :2)"
            self._cursor.setinputsizes(None, 200)
            self._cursor.executemany(query, param)
            self.commit_execution()
            # closing database connection
            self.close_connection()
            log.log_info("Execution Batch Query Successful")
            print("Execution Batch Query Successful")
            return 1
        except Exception as e:
            log.log_critical("Execution Failed Due To " + str(e))
            self.close_connection()
            print(e)
            return 0
        except cx_Oracle.DatabaseError as e:
            log.log_critical("Execution Failed Due To " + str(e))
            self.close_connection()
            print(e)
            return 0

        # if :
        #     return max(paths, key=os.path.getctime)
        # else:
        #     time.sleep(3)
        #     print('Waiting 3 Second to get files from download....')
        #     self.newest_file_downloaded(self)
