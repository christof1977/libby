#!/usr/bin/env python3

import syslog
import pymysql
import time

import logging
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

class mysqldose(object):
    def __init__(self, mysqluser, mysqlpass, mysqlserv, mysqldb):
        self.mysqluser = mysqluser
        self.mysqlpass = mysqlpass
        self.mysqlserv = mysqlserv
        self.mysqldb = mysqldb
        self.start()

    def __del__(self):
        self.close()

    def start(self):
        self.mysql_success = False
        try:
            #self.cnx = MySQLdb.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
            self.cnx = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
            self.mysql_success = True
            logging.info("Database connection established")
        except Exception as e:
            try:
                self.mysql_success = False
                logging.error("Database connection error: "+str(e))
                print("1: ",str(e))
                self.cnx.disconnect()
            except Exception as e:
                print("2: ",str(e))
                pass

    def close(self):
        if self.mysql_success == True:
            self.cnx.close()
            logging.info("DB Connection terminated")

    def read_one(self, parameter, datetime = None):
        # reads a single value of the messwert table
        # parameter is the parameter to read
        # datetime can be empty or specified like "YYYY-MM-DD HH:mm:ss"
        if self.mysql_success == True:
            try:
                self.cursor = self.cnx.cursor()
                if datetime == None:
                    query ='SELECT value FROM messwert WHERE parameter = %s ORDER BY `index` DESC LIMIT 0, 1;'
                    result = self.cursor.execute(query, (parameter,))
                else:
                    query ='SELECT value FROM messwert WHERE parameter = %s AND datetime = %s;'
                    result = self.cursor.execute(query, (parameter, datetime))
                row = self.cursor.fetchall()
                self.cnx.commit()
                self.cursor.close()
                #print(result)
                return row[0][0]
            except Exception as e:
            #except (AttributeError, MySQLdb.OperationalError):
                logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
                self.mysql_success = False
        else:
            self.start()

    def read_many(self, parameter, datetime):
        # reads many values of the messwert table
        # parameter is the parameter to read
        # datetime is speciefied as "2018-10-16%" for a whole day as example
        if self.mysql_success == True:
            try:
                self.cursor = self.cnx.cursor()
                query ='SELECT value, datetime FROM messwert WHERE parameter = %s AND datetime LIKE %s;'
                result = self.cursor.execute(query, (parameter, datetime))
                result = self.cursor.fetchall()
                self.cnx.commit()
                self.cursor.close()
                values = [elt[0] for elt in result]
                datetimes = [elt[1] for elt in result]
                #print(datetimes)
                return values, datetimes
            except Exception as e:
            #except (AttributeError, MySQLdb.OperationalError):
                logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
        else:
            self.start()

    def calc_pwr_h(self, datetime):
        # Calculates the collected solar power within a given hour
        # datetime hast to specified as follows: "2018-09-11 11%"
        if self.mysql_success == True:
            try:
                self.cursor = self.cnx.cursor()
                query ='SELECT ROUND(SUM(value)/COUNT(value), 3) FROM messwert WHERE datetime LIKE %s AND parameter = %s;'
                result = self.cursor.execute(query, (datetime, "OekoKollLeistung"))
                row = self.cursor.fetchall()
                self.cnx.commit()
                self.cursor.close()
                return row[0][0]
            except Exception as e:
            #except (AttributeError, MySQLdb.OperationalError):
                logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
        else:
            self.start()

    def write(self, now, parameter, value):
        # writes to the messwert table of the database
        # now = time.strftime('%Y-%m-%d %H:%M:%S')
        # parameter: char(50)
        # value: float
        if now == "now":
            now = time.strftime('%Y-%m-%d %H:%M:%S')
        logging.debug("Opening connection to DB")
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        add = ("INSERT INTO messwert "
                "(datetime, parameter, value) "
                "VALUES (%s, %s, %s)")
        data = (now, parameter, value)
        logging.debug(data)
        try:
            with con.cursor() as cur:
                logging.debug("Executing query")
                cur.execute(add, data)
                mess_id = cur.lastrowid
                con.commit()
        except Exception as e:
            logging.error("Fehler beim Schreiben in die Datenbank: "+str(e))
            self.close()
            self.start()
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        return mess_id

if __name__ == "__main__":
    dbconn = mysqldose('heizung', 'heizung', 'dose.fritz.box', 'heizung')
    dbconn.start()
    #dbconn.write('2017-11-12 1:2:3', 'Test', 44.0)
    #result = dbconn.read_one("OekoKollLeistung", "2018-09-12 18:42:25")
    #result = dbconn.calc_pwr_h("2018-09-12 17%")
    #print(result)
    dbconn.read_many("OekoKollLeistung", "2018-10-16%")
    dbconn.close()
