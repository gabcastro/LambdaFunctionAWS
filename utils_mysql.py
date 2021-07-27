import sys
import logging
import pymysql

class MySqlDataManager(object):
    
    def __init__(self, db_connection):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
        self.host = '{}'.format(db_connection['db_host'])
        self.user = '{}'.format(db_connection['db_user'])
        self.passwd = '{}'.format(db_connection['db_password'])
        self.db = '{}'.format(db_connection['db_name'])
        
        self.conn = self.open_conn()
        

    def open_conn(self):
        try:
            conn = pymysql.connect(host=self.host, port=3306, user=self.user, passwd=self.passwd, db=self.db, connect_timeout=10)
        except pymysql.MySQLError as e:
            self.logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
            self.logger.error(e)
        except Exception as e:
            self.logger.error("ERROR: Unexpected error.")
            self.logger.error(e)
        
        self.logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
        return conn
        
    def execute_update(self, script):
        message = None
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            result = True
        except Exception as e:
            self.logger.error("ERROR: Unexpected error.")
            self.logger.error(e)
            message = e
            result = False
        finally:
            cur.close()
            self.conn.close()
            
        self.logger.info("SUCCESS: Update completed")
        return (result, message)
        