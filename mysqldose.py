#!/usr/bin/env python3

import syslog
import pymysql
import time
import datetime
import numpy as np

import logging
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

try:
    import datevalues
except ImportError:
    import sys
    from . import datevalues

class Mysqldose(object):
    '''
    The class Mysqldose provides the connection to the MySQL/MariaDB-Server on
    dose, in particular the database heizung. This database stores all logged
    data of the house. Data could be written or read from the db. Moreover, the
    class provides methods for daily maintenance and caluclations.
    '''
    def __init__(self, mysqluser, mysqlpass, mysqlserv, mysqldb):
        self.mysqluser = mysqluser
        self.mysqlpass = mysqlpass
        self.mysqlserv = mysqlserv
        self.mysqldb = mysqldb

    def read_one(self, parameter, dt = None):
        '''
        Reads a single value of the messwert table
            parameter is the parameter to read
            dt can be empty or specified like "YYYY-MM-DD HH:mm:ss"
            If dt left empty, the most recent value is returned

        Returns a single value.
        '''
        logging.debug("Opening connection to DB")
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                logging.debug("Executing query")
                if dt == None:
                    query ='SELECT messwert.value, messwert.index \
                            FROM parameter \
                            JOIN messwert \
                            ON messwert.parameter = parameter.parid \
                            WHERE parameter.parameter = %s \
                            ORDER BY messwert.index DESC LIMIT 0, 1;'
                    cur.execute(query, (parameter,))
                else:
                    query ='SELECT messwert.value, messwert.datetime \
                            FROM parameter \
                            JOIN messwert ON messwert.parameter = parameter.parid \
                            WHERE parameter.parameter = %s \
                            AND messwert.datetime > %s;'
                    cur.execute(query, (parameter, dt))
                row = cur.fetchall()
        except Exception as e:
            logging.error("Error while reading from DB: "+str(e))
            self.mysql_success = False
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        if(row):
                return row[0][0]
        else:
            return("Error")

    def read_many(self, parameter, dt):
        '''
        Reads many values of the messwert table
            parameter is the parameter to read
            dt is specified as "2018-10-16%" for a whole day as example

        Returns lists of values and corresponding datetimes
        '''
        logging.debug("Opening connection to DB")
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                logging.debug("Executing query")
                query ='SELECT value, datetime FROM messwert WHERE parameter = %s AND datetime LIKE %s LIMIT 10;'
                print(query % (parameter, dt))
                cur.execute(query, (parameter, dt))
                logging.debug("Executing commit")
                con.commit()
                logging.debug("Executing fetchall")
                result = cur.fetchall()
                logging.debug(result)
                values = [elt[0] for elt in result]
                datetimes = [elt[1] for elt in result]
        except Exception as e:
            logging.error("Error while reading from DB: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        return values, datetimes

    def write(self, now, parameter, value):
        '''
        Writes to the messwert table of the database
        now = time.strftime('%Y-%m-%d %H:%M:%S') or just "now"
        parameter: char(50)
        value: float
        '''
        if now == "now":
            now = time.strftime('%Y-%m-%d %H:%M:%S')
        logging.debug("Opening connection to DB")
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        add = ("INSERT INTO messwert "
                "(datetime, parameter, value) "
                "VALUES (%s, %s, %s)")
        paridQ = "SELECT parid from parameter WHERE parameter = %s;"
        try:
            with con.cursor() as cur:
                cur.execute(paridQ, (parameter))
                parid = cur.fetchall()
                if(len(parid) == 0):
                    insParQ = ("INSERT INTO parameter (parameter) \
                               VALUES (%s);")
                    res = cur.execute(insParQ, (parameter))
                    con.commit()
                    cur.execute(paridQ, (parameter))
                    parid = cur.fetchall()
                parid = parid[0][0]
                mess_id = cur.lastrowid
                con.commit()
                logging.debug("Executing query")
                data = (now, parid, value)
                logging.debug(data)
                res = cur.execute(add, data)
                con.commit()
        except Exception as e:
            logging.error("Error while writing to DB: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        return mess_id

    def insert_daily_row(self, day):
        '''
        This function checks, if the table entry for the given day already
        exists; if not, the entry is created. Important for the daily calc
        functions.
        '''
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                query = 'SELECT * FROM daily WHERE day = %s'
                cur.execute(query, day)
                rows = cur.rowcount
                if(rows == 0):
                    query = 'INSERT INTO daily (day) \
                            VALUES (%s)'
                    cur.execute(query, day.date())
                    con.commit()
        except Exception as e:
            logging.error("Reading from DB: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()

    def read_day(self, day, parameter):
        '''
        This function reads all values of parameter for a given day in as  date
        object and returns the result array.
        '''
        end_date = day + datetime.timedelta(hours=23, minutes=59, seconds=59)
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                query = 'SELECT messwert.index, messwert.datetime,\
                        messwert.value \
                        FROM parameter \
                        JOIN messwert \
                        ON messwert.parameter = parameter.parid \
                        WHERE datetime BETWEEN %s AND %s \
                        AND parameter.parameter = %s'
                cur.execute(query, (day, end_date, parameter))
                res = cur.fetchall()
        except Exception as e:
            logging.error("Error while reading from DB: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        return(res)

    def write_day(self, day, parameter, value):
        '''
        This function writes the daily value of a given parameter to the daily
        table.
        '''
        self.insert_daily_row(day)
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                update = 'UPDATE daily SET {} = %s \
                          WHERE day = %s;'
                cur.execute(update.format(parameter), (value, day))
                con.commit()
        except Exception as e:
            logging.error("Error while writing to DB: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()

    def delete_redundancy(self, parameter, day=None):
        '''
        This function is intended to delete redundant values from the database with a given parameter.
        A value is consideres as redundant, if it is unchanged compared to the previous value.
        '''
        logging.debug("Deleting redundant values of {}".format(parameter))
        start_date, end_date = datevalues.date_values(day)
        res = self.read_day(start_date, parameter)
        value = 0
        idx_del = []
        idx_nodel = []
        for line in res:
            if(line[2] == value):
                idx_del.append(line[0])
            else:
                idx_nodel.append(line[0])
            value = line[2]
        del_query = 'DELETE FROM messwert WHERE `index` = %s LIMIT 1;'
        for id_del in idx_del:
            con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
            try:
                logging.debug("Deleting %s" % id_del)
                with con.cursor() as cur:
                    cur.execute(del_query, id_del)
                    con.commit()
            except Exception as e:
                logging.error("Error while deleting record: "+str(e))
            finally:
                logging.debug("Closing connection to DB")
                con.close()
        logging.info("Deleted {} redundant values of {}".format(len(idx_del), parameter))

if __name__ == "__main__":
    pass
