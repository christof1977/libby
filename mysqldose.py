#!/usr/bin/env python3

import syslog
import MySQLdb
import time
if __name__ == "__main__":
    from logger import logger
else:
    from .logger import logger

logging = True

class mysqldose(object):
    pass

    def __init__(self, mysqluser, mysqlpass, mysqlserv, mysqldb):
        self.mysqluser = mysqluser
        self.mysqlpass = mysqlpass
        self.mysqlserv = mysqlserv
        self.mysqldb = mysqldb

    def start(self):
        self.mysql_success = False
        try:
            self.cnx = MySQLdb.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
            self.mysql_success = True
            logger("Database connection established", logging)
        except Exception as e:
            try:
                self.mysql_success = False
                logger("Database connection error: "+str(e), logging)
                print("1: ",str(e))
                self.cnx.disconnect()
            except Exception as e:
                print("2: ",str(e))
                pass

    def close(self):
        if self.mysql_success == True:
            self.cnx.close()
            logger("DB Connection terminated", logging)

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
                logger("Fehler beim lesen aus der Datenbank: "+str(e), logging)

        else:
            self.start()

    def calc_pwr_h(self, datetime):
        # Calculates the collected solar ower within a given hour
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
                logger("Fehler beim lesen aus der Datenbank: "+str(e), logging)
        else:
            self.start()



    def write(self, now, parameter, value):
        # writes to the messwert table of the database
        # now = time.strftime('%Y-%m-%d %H:%M:%S')
        # parameter: char(50)
        # value: float
        if self.mysql_success == True:
            add = ("INSERT INTO messwert " 
                    "(datetime, parameter, value) "
                    "VALUES (%s, %s, %s)")
            data = (now, parameter, value)
            try:
                self.cursor = self.cnx.cursor()
                self.cursor.execute(add, data)
                mess_id = self.cursor.lastrowid
                self.cnx.commit()
                self.cursor.close()
                return mess_id
                #print(add)
            except Exception as e:
                logger("Fehler beim Schreiben in die Datenbank: "+str(e), logging)
                self.close()
                self.start()
        else:
            self.start()



if __name__ == "__main__":
    dbconn = mysqldose('heizung', 'heizung', 'dose.fritz.box', 'heizung') 
    dbconn.start()
    #dbconn.write('2017-11-12 1:2:3', 'Test', 44.0)
    #result = dbconn.read_one("OekoKollLeistung", "2018-09-12 18:42:25")
    result = dbconn.calc_pwr_h("2018-09-12 17%")
    print(result)
    dbconn.close()
