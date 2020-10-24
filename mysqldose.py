#!/usr/bin/env python3

import sys
import getopt
import syslog
import pymysql
import time
from datetime import datetime, date, timedelta
import numpy as np

import logging
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

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

    def write(self, now, parameter, value):
        '''
        writes to the messwert table of the database
        now = time.strftime('%Y-%m-%d %H:%M:%S')
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
            logging.error("Fehler beim Schreiben in die Datenbank: "+str(e))
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
        end_date = day + timedelta(hours=23, minutes=59, seconds=59)
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
            logging.error("Fehler beim Schreiben in die Datenbank: "+str(e))
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
            logging.error("Fehler beim Schreiben in die Datenbank: "+str(e))
        finally:
            logging.debug("Closing connection to DB")
            con.close()

    def date_values(self, day=None):
        '''
        Returns a datetime object for given day or today with time 00:00:00 as
        start_date and time 23:59:50 as end_date
        '''
        if(day is None):
            start_date = datetime.combine(date.today(), datetime.min.time())
        else:
            try:
                start_date = datetime.strptime(day, "%Y-%m-%d")
            except:
                logging.error("Not a valid date")
                return(-1)
        end_date = start_date + timedelta(hours=23, minutes=59, seconds=59)
        return(start_date, end_date)

    def update_solar_gain(self, day=None):
        '''
        This function reads the solar gain of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        start_date, end_date = self.date_values(day)
        res = self.read_day(start_date, "OekoKollLeistung")
        pwr = []
        for line in res:
            pwr.append(line[2])
        pwr = round(sum(pwr)/(3600/(86400/len(pwr))), 3)
        self.write_day(start_date, "Solarertrag", pwr)

    def update_pellet_consumption(self, day=None):
        '''
        This function reads the pellet consumption of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        start_date, end_date = self.date_values(day)
        res = self.read_day(start_date, "OekoStoragePopper")
        val = [0]
        for line in res:
            try:
                if(line[2] != val[-1]):
                    val.append(line[2])
            except:
                pass
        val.pop(0)
        val = np.array(val)
        diffval = np.diff(val)
        noval = len(diffval)
        diffval = diffval[diffval != -1]
        verbrauch = noval - len(diffval)
        self.write_day(start_date, "VerbrauchPellets", verbrauch)

    def update_heating_energy(self, parameter, day=None):
        '''
        This function reads the value of the heating power consumption of a
        given day or today and the value of the day before and calculates the
        power consuption of this day. The values is stored in the daily table.
        '''
        start_date, end_date = self.date_values(day)
        try:
            con_today  = self.read_day(start_date, parameter)[0][2]
            yesterday = start_date - timedelta(1)
            con_yesterday  = self.read_day(yesterday, parameter)[0][2]
            con = (con_today - con_yesterday)/1000
            self.write_day(start_date, parameter, con)
        except Exception as e:
            logging.error("Something went wrong: " + str(e))

if __name__ == "__main__":
    day = None
    update = False
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'd:u')
    except getopt.GetoptError as err:
        logging.error("Arguments error!")
        exit()
    for o,a in opts:
        if(o == "-d"):
            if(a == "yesterday"):
                day = date.today() - timedelta(1)
                day = day.strftime("%Y-%m-%d")
            else:
                try:
                    day = datetime.strptime(a, "%Y-%m-%d")
                except:
                    logging.error("Invalid date format. Use something like 2020-10-10. Bye")
                    exit()
        elif(o == "-u"):
            update = True

    dbconn = mysqldose('heizung', 'heizung', 'dose', 'heizung')
    if(update):
        logging.info("Performing daily database updates")
        dbconn.update_solar_gain(day=day)
        dbconn.update_pellet_consumption(day=day)
        dbconn.update_heating_energy("VerbrauchHeizungEg", day=day)

    logging.info("Bye.")



    #dbconn.write('2017-11-12 1:2:3', 'Test', 44.0)
    #result = dbconn.read_one("OekoKollLeistung", "2018-09-12")
    #dbconn.update_pellet_consumption(day="2020-10-23")
    #dbconn.update_heating_energy("VerbrauchHeizungEg",day="2020-10-23")
    #print(dbconn.read_many("OekoAussenTemp", "2020-10-18 18:01%"))
    #print(dbconn.read_one("OekoAussenTemp"))
