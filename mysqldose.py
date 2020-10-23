#!/usr/bin/env python3

import syslog
import pymysql
import time

import logging
#logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)

class mysqldose(object):
    def __init__(self, mysqluser, mysqlpass, mysqlserv, mysqldb):
        self.mysqluser = mysqluser
        self.mysqlpass = mysqlpass
        self.mysqlserv = mysqlserv
        self.mysqldb = mysqldb

    def read_one(self, parameter, datetime = None):
        # reads a single value of the messwert table
        # parameter is the parameter to read
        # datetime can be empty or specified like "YYYY-MM-DD HH:mm:ss"
        logging.debug("Opening connection to DB")
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                logging.debug("Executing query")
                if datetime == None:
                    query ='SELECT messwert.value \
                            FROM parameter \
                            JOIN messwert \
                            ON messwert.parameter = parameter.parid \
                            WHERE parameter.parameter = %s \
                            ORDER BY `messwert.index` DESC LIMIT 0, 1;'
                    cur.execute(query, (parameter,))
                else:
                    query ='SELECT messwert.value, messwert.datetime \
                            FROM parameter \
                            JOIN messwert ON messwert.parameter = parameter.parid \
                            WHERE parameter.parameter = %s \
                            AND messwert.datetime LIKE %s;'
                    cur.execute(query, (parameter, datetime))
                row = cur.fetchall()
        except Exception as e:
            logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
            self.mysql_success = False
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        return row[0][0]

    def read_many(self, parameter, datetime):
        # reads many values of the messwert table
        # parameter is the parameter to read
        # datetime is specified as "2018-10-16%" for a whole day as example
        logging.debug("Opening connection to DB")
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                logging.debug("Executing query")
                query ='SELECT value, datetime FROM messwert WHERE parameter = %s AND datetime LIKE %s LIMIT 10;'
                print(query % (parameter, datetime))
                cur.execute(query, (parameter, datetime))
                logging.debug("Executing commit")
                con.commit()
                logging.debug("Executing fetchall")
                result = cur.fetchall()
                logging.debug(result)
                #self.cnx.commit()
                #self.cursor.close()
                values = [elt[0] for elt in result]
                datetimes = [elt[1] for elt in result]
                #print(datetimes)
        except Exception as e:
            #except (AttributeError, MySQLdb.OperationalError):
            logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        return values, datetimes

    def calc_pwr(self, year, month=None, day=None, hour=None):
        # Calculates the collected solar power within a given hour
        # datetime hast to specified as follows: "2018-09-11 11%"
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        if(month is None):
            start = str(year) + "-01-01"
            end = str(year + 1) + "-01-01"
        elif(day is None):
            if(month < 1 or month > 12):
                return
            else:
                start = str(year) + "-" + str(month) + "-01"
            if(month == 12):
                end = str(year + 1) + "-01-01" 
            else:
                end = str(year) + "-" + str(month + 1) + "-01" 
        elif(hour is None):
            if(day < 1 or day > 31):
                return
            else:
                start = str(year) + "-" + str(month) + "-" + str(day)
                end = str(year) + "-" + str(month) + "-" + str(day + 1)


       # if(hour is None):
       #     end_month = month + 1
       #     if(end_month > 12):
       #         end
       #     start = str(year) + "-" + str(month)
       #     end = str(year) + "-" + str(month + 1)

        #start = day + " " + str(hour)
        #end = day + " " + str(hour + 1)
        print(start)
        print(end)
        #try:
        #    with con.cursor() as cur:
        #        query ='SELECT ROUND(SUM(value)/COUNT(value), 3) FROM messwert WHERE parameter = %s AND datetime BETWEEN %s AND %s'
        #        cur.execute(query, ("OekoKollLeistung", start, end))
        #        row = cur.fetchall()
        #        con.commit()
        #except Exception as e:
        #    logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
        #finally:
        #    con.close()
        #return row[0][0]

    def calc_pwr_month(self, year, month):
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        start = str(year) + "-" + str(month) + "-01"
        try:
            with con.cursor() as cur:
                query = "SELECT \
                        ROUND(SUM(value)/(3600/(86400/COUNT(value))) * \
                        DATEDIFF(DATE_ADD(%s, INTERVAL 1 MONTH), %s), 3) \
                        FROM messwert WHERE parameter = %s AND \
                        datetime BETWEEN %s AND \
                        DATE_ADD(%s, INTERVAL 1 MONTH);"
                #query ='SELECT ROUND(SUM(value)/COUNT(value), 3) FROM messwert WHERE parameter = %s AND datetime BETWEEN %s AND DATE_ADD(%s, INTERVAL 1 MONTH)'
                cur.execute(query, (start, start, "OekoKollLeistung", start, start))
                row = cur.fetchall()
                con.commit()
        except Exception as e:
            logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
        finally:
            con.close()
        return row[0][0]

    def calc_pwr_year(self, year):
        power = []
        for month in range(1,13):
            result = self.calc_pwr_month(year, month)
            if(result is not None):
                power.append(result)
            print(month, ":",result, "kWh")
        return(sum(power))

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
            logging.error("Fehler beim Schreiben in die Datenbank: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        return mess_id

if __name__ == "__main__":
    dbconn = mysqldose('heizung', 'heizung', 'dose', 'heizung')
    #dbconn.write('2017-11-12 1:2:3', 'Test', 44.0)
    result = dbconn.read_one("OekoKollLeistung", "2018-09-12")
    print(result)
    #result = dbconn.calc_pwr("2020-09", day= 12, hour=12)
    #result = dbconn.calc_pwr(2019, month=11, day = 3)
    #print(dbconn.read_many("OekoAussenTemp", "2020-10-18 18:01%"))
    #print(dbconn.read_one("OekoAussenTemp"))
    #print(dbconn.calc_pwr_year(2020), "kWh")
