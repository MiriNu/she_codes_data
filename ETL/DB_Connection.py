import psycopg2 as pg
import sys
import os
from configparser import ConfigParser
from io import StringIO
import pandas as pd

class DBconnection:

    def __init__(self):
        self.conn = None
        self.cur = None
        self.connect()


    def config(self,filename='./configs/database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
            print(db)
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            params = self.config()
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = pg.connect(**params)
            # create a cursor
            self.cur = self.conn.cursor()
            # execute a statement
            print('PostgreSQL database version:')
            self.cur.execute('SELECT version()')
            # display the PostgreSQL database server version
            db_version = self.cur.fetchone()
            print(db_version)
            # close the communication with the PostgreSQL
            self.cur.close()
        except (Exception, pg.DatabaseError) as error:
            print(error)

    def disconnect(self):
        if self.conn is not None:
            self.conn.close()
            print("Disconnected")


    def insert_into_db(self, df, schema,table):
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False)
        buffer.seek(0)

        try:
            self.cur = self.conn.cursor()
            self.cur.copy_from(buffer, schema+'.'+table, sep=",")
            self.conn.commit()
        except (Exception, pg.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            self.cur.close()
            return 1
        print("copy_from_stringio() done")
        self.cur.close()
        return 0