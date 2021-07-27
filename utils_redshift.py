import sys
import logging
import psycopg2

from utils import Messages

class RedshiftDataManager(object):
    
    def __init__(self, db_connection):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
        self.connection = self.conn_string(db_connection)
    
    def execute_update(self, script):
        message = None
        try:
            cur = self.connection.cursor()
            cur.execute(script)
            self.connection.commit()
            result = True
        except Exception as e:
            Messages.print_message(e)
            self.connection.rollback()
            message = e
            result = False
        finally:
            self.connection.close()
            
        return (result, message)
        
    
    def conn_string(self, db_connection):
        return "dbname='{}' port='5439' user='{}' password='{}' host='{}'".format(db_connection['db_name'], db_connection['db_user'], db_connection['db_password'], db_connection['db_host'])
        
        
    def open_conn(self):
        try:
            conn = psycopg2.connect(self.connection)
        except psycopg2.errors.lookup("08006"):
            self.logger.error("ERROR: Unexpected error: Could not connect to Redshift instance.")
            sys.exit()
            
        self.logger.info("SUCCESS: Connection to Redshift instance succeeded")
        return conn
        
            
    def run_update(self, script):
        conn = self.open_conn()
        return self.execute_update(conn, conn.cursor(), script)