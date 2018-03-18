#!/usr/bin/env python3

import syslog
import mysql.connector


logging = False

def logger(msg):
    if logging == True:
        print(msg)
        syslog.syslog(str(msg))


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
            self.cnx.close()
            logger("DB Connection terminated")

    def read_latest(self, parameter):
        if self.mysql_success == True:
            try:
                #query = 'SELECT MAX(index), value from messwert WHERE parameter = %s LIMIT 1'
                query ='SELECT value FROM messwert WHERE parameter = %s  ORDER BY `index` DESC LIMIT 0, 1;'
                result = self.cursor.execute(query, (parameter,))
                row = self.cursor.fetchall()
                self.cnx.commit()
                #print(result)
                #for value in result:
                return row[0][0]
            except Exception as e:
                logger("Fehler beim lesen aus der Datenbank")
        else:
            self.start()



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
                self.start()
        else:
            self.start()

if __name__ == "__main__":
    dbconn = mysqldose('heizung', 'heizung', 'dose.fritz.box', 'heizung') 
    dbconn.start()
    #dbconn.write('2017-11-12 1:2:3', 'TerrasseTemp', 44.0)
    result = dbconn.read_latest("WohnzimmerTemp")
    print(result)
    dbconn.close()
