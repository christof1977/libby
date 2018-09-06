#!/usr/bin/env python3

import syslog
import mysql.connector


logging = True

def logger(msg):
    if logging == True:
        print(msg)
        syslog.syslog(str(msg))


class mysqlcollect(object):
    pass

    def __init__(self, mysqluser, mysqlpass, mysqlserv, mysqldb):
        self.mysqluser = mysqluser
        self.mysqlpass = mysqlpass
        self.mysqlserv = mysqlserv
        self.mysqldb = mysqldb

    def start(self):
        self.mysql_success = False
        try:
            self.cnx = mysql.connector.connect(user=self.mysqluser, password=self.mysqlpass,host=self.mysqlserv,database=self.mysqldb)
            self.cursor = self.cnx.cursor()
            self.mysql_success = True
            logger("Database connection established")
        except Exception as e:
            try:
                self.mysql_success = False
                logger("Database connection error")
                self.cnx.disconnect()
            except Exception as e:
                pass



    def close(self):
        if self.mysql_success == True:
            self.cursor.close()
            logger("DB Connection terminated")

    def write(self, now, parameter, value):
        if self.mysql_success == True:
            #logger("Writing to DB")
            add = ("INSERT INTO messwert " 
                    "(datetime, parameter, value) "
                    "VALUES (%s, %s, %s)")
            
            #add = ("INSERT INTO testkollekte " 
            #        "(datetime, parameter, value) "
            #        "VALUES (%s, %s, %s)")


            #data = (time.strftime('%Y-%m-%d %H:%M:%S'), parameter, value)
            data = (now, parameter, value)
            try:
                self.cursor.execute(add, data)
                mess_id = self.cursor.lastrowid
                self.cnx.commit()
                return mess_id
                print(add)
            except Exception as e:
                logger("Fehler beim Schreiben in die Datenbank")
                self.stop()
                self.start()
        else:
            self.start()

if __name__ == "__main__":
    dbconn = mysqlcollect('heizung', 'heizung', 'dose.fritz.box', 'garagntest') 
    dbconn.start()
    dbconn.write('2017-11-12 1:2:3', 'TerrasseTemp', 44.0)
    dbconn.close()
