#!/usr/bin/env python3

import syslog
import mysql.connector
import time

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

    def read_one(self, parameter, datetime = None):
        # reads a single value of the messwert table
        # parameter is the parameter to read
        # datetime can be empty or specified like "YYYY-MM-DD HH:mm:ss"
        if self.mysql_success == True:
            try:
                if datetime == None:
                    query ='SELECT value FROM messwert WHERE parameter = %s ORDER BY `index` DESC LIMIT 0, 1;'
                    result = self.cursor.execute(query, (parameter,))
                    #print("ohne datetime")
                else:
                    query ='SELECT value FROM messwert WHERE parameter = %s AND datetime = %s;'
                    result = self.cursor.execute(query, (parameter, datetime))
                    #print("mit datetime")
                row = self.cursor.fetchall()
                self.cnx.commit()
                #print(result)
                #for value in result:
                return row[0][0]
            except Exception as e:
                logger("Fehler beim lesen aus der Datenbank")
        else:
            self.start()

    def calc_pwr_h(self, datetime):
        # Calculates the collected solar ower within a given hour
        # datetime hast to specified as follows: "2018-09-11 11%"
        if self.mysql_success == True:
            try:
                query ='SELECT ROUND(SUM(value)/COUNT(value), 3) FROM messwert WHERE datetime LIKE %s AND parameter = %s;'
                result = self.cursor.execute(query, (datetime, "OekoKollLeistung"))
                row = self.cursor.fetchall()
                self.cnx.commit()
                #print("Last Query:", self.cursor._last_executed)
                #print(result)
                #for value in result:
                return row[0][0]
                #return row
            except Exception as e:
                #print("Last Query:", self.cursor._last_executed)
                logger("Fehler beim lesen aus der Datenbank")
        else:
            self.start()



    def write(self, now, parameter, value):
        # writes to the messwert table of the database
        # now = time.strftime('%Y-%m-%d %H:%M:%S')
        # parameter: char(50)
        # value: float
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
                #print(add)
            except Exception as e:
                logger("Fehler beim Schreiben in die Datenbank")
                self.stop()
                self.start()
        else:
            self.start()



if __name__ == "__main__":
    dbconn = mysqldose('heizung', 'heizung', 'dose.fritz.box', 'heizung') 
    dbconn.start()
    #dbconn.write('2017-11-12 1:2:3', 'TerrasseTemp', 44.0)
    #result = dbconn.read_one("OekoKollLeistung", "2018-09-12 18:42:25")
    result = dbconn.calc_pwr_h("2018-09-12 17%")
    print(result)
    dbconn.close()
