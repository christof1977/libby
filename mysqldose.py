#!/usr/bin/env python3

import os
import sys
import getopt
import syslog
import configparser
import pymysql
import time
import datetime
import numpy as np
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

import logging
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

try:
    import datevalues
except ImportError:
    import sys
    from . import datevalues

configfile = "collectdata.ini"
realpath = os.path.realpath(__file__)
basepath = os.path.split(realpath)[0]
configfile = os.path.join(basepath, configfile)

class Mysqldose(object):
    '''
    The class Mysqldose provides the connection to the MySQL/MariaDB-Server on
    dose, in particular the database heizung. This database stores all logged
    data of the house. Data could be written or read from the db. Moreover, the
    class provides methods for daily maintenance and caluclations.
    '''
    def __init__(self, mysqluser, mysqlpass, mysqlserv, mysqldb):
        self.read_config()
        #self.mysqluser = mysqluser
        #self.mysqlpass = mysqlpass
        #self.mysqlserv = mysqlserv
        #self.mysqldb = mysqldb

    def read_config(self):
        try:
            self.config = configparser.ConfigParser()
            self.config.read(configfile)
            self.basehost = self.config['BASE']['Host']
            self.baseport = int(self.config['BASE']['Port'])
            self.mysqluser = self.config['BASE']['Mysqluser']
            self.mysqlpass = self.config['BASE']['Mysqlpass']
            self.mysqlserv = self.config['BASE']['Mysqlserv']
            self.mysqldb = self.config['BASE']['Mysqldb']
            self.influxserv = self.config['BASE']['Influxserv']
            self.influxtoken = self.config['BASE']['Influxtoken']
            self.influxbucket = self.config['BASE']['Influxbucket']
            self.influxorg = self.config['BASE']['Influxorg']
        except Exception as e:
            logging.error("Configuration error")
            logging.error(e)

    def read_one(self, parameter, dt = None):
        # reads a single value of the messwert table
        # parameter is the parameter to read
        # datetime can be empty or specified like "YYYY-MM-DD HH:mm:ss"
        logging.debug("Opening connection to DB")
        con = pymysql.connect(user=self.mysqluser, passwd=self.mysqlpass,host=self.mysqlserv,db=self.mysqldb)
        try:
            with con.cursor() as cur:
                logging.debug("Executing query")
                if datetime == None:
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
            logging.error("Fehler beim lesen aus der Datenbank: "+str(e))
            self.mysql_success = False
        finally:
            logging.debug("Closing connection to DB")
            con.close()
        if(row):
                return row[0][0]
        else:
            return("Error")

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

    def update_solar_gain(self, day=None):
        '''
        This function reads the solar gain of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        logging.info("Calculating solar gain and writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
        res = self.read_day(start_date, "OekoKollLeistung")
        pwr = []
        for line in res:
            pwr.append(line[2])
        try:
            pwr = round(sum(pwr)/(3600/(86400/len(pwr))), 3)
        except ZeroDivisionError:
            pwr = 0
        self.write_day(start_date, "Solarertrag", pwr)

    def update_pellet_consumption(self, day=None):
        '''
        This function reads the pellet consumption of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        logging.info("Calculating pellet consumption and writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
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
        logging.info("Getting consumed heating energy of day and  writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
        try:
            con_today  = self.read_day(start_date, parameter)[0][2]
            yesterday = start_date - datetime.timedelta(1)
            con_yesterday  = self.read_day(yesterday, parameter)[0][2]
            con = (con_today - con_yesterday)/1000
            self.write_day(start_date, parameter, con)
        except Exception as e:
            logging.error("Something went wrong: " + str(e))

    def influx_query2(self, day=None):
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        start_date, end_date = datevalues.date_values_influx(day)
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "E3DC/BAT_DATA/0/BAT_USABLE_CAPACITY" and r["_field"] == "value") \
                      |> max()'
        result = query_api.query(query)
        for table in result:
            for record in table:
                max_cap = round(record.get_value(), 2)
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "E3DC/DCDC_DATA/0/DCDC_U_BAT" and r["_field"] == "value") \
                      |> max()'
        result = query_api.query(query)
        for table in result:
            for record in table:
                max_voltage = round(record.get_value(), 2)
        print(max_cap, max_voltage, round(max_cap*max_voltage*0.9/1000,2))

    def influx_calc_energy(self, day=None):
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        start_date, end_date = datevalues.date_values_influx(day)
        print(start_date)
        return
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "E3DC/EMS_DATA/EMS_POWER_HOME" and r["_field"] == "value")'
        result = query_api.query(query)
        for table in result:
            for record in table:
                print(record)

    def influx_query(self, parameter, fil, day=None):
        start_date, end_date = datevalues.date_values_influx(day)
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "'+parameter+'" and r["_field"] == "value")'
        if(fil in ["pos", "neg"]):
            if(fil=="pos"):
                query = query + '|> filter(fn: (r) => r["_value"] > 0.0)'
            else:
                query = query + '|> filter(fn: (r) => r["_value"] <= 0.0)'
        query = query + ' |> aggregateWindow( \
                            every: 1h, \
                            fn: (tables=<-, column) => \
                                tables \
                                    |> integral(unit: 1h) \
                                    |> map(fn: (r) => ({ r with _value: r._value / 1000.0}))) \
                      |> aggregateWindow(fn: sum, every: 1mo) '
        return query

    def update_electrical(self, day=None):
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        parameter = {"VerbrauchStromHaus":{"par":"E3DC/EMS_DATA/EMS_POWER_HOME","filter":None},
                "PVErtragAc":{"par":"E3DC/EMS_DATA/EMS_POWER_PV","filter":None},
                "PVErtragDcOst":{"par":"E3DC/PVI_DATA/0/PVI_DC_POWER/0/PVI_VALUE","filter":None},
                "PVErtragDcWest":{"par":"E3DC/PVI_DATA/0/PVI_DC_POWER/1/PVI_VALUE","filter":None},
                "Netzbezug":{"par":"E3DC/EMS_DATA/EMS_POWER_GRID","filter":"pos"},
                "Netzeinspeisung":{"par":"E3DC/EMS_DATA/EMS_POWER_GRID","filter":"neg"},
                "Batterieladung":{"par":"E3DC/EMS_DATA/EMS_POWER_BAT","filter":"pos"},
                "Batterieentladung":{"par":"E3DC/EMS_DATA/EMS_POWER_BAT","filter":"neg"}}
        for key in parameter:
            result = query_api.query(self.influx_query(parameter[key]["par"], fil=parameter[key]["filter"] , day=day))
            for table in result:
                for record in table:
                    value = round(record.get_value(), 2)
            start_date, end_date = datevalues.date_values(day)
            logging.info("Calculating {} of day and writing value to daily table".format(key))
            self.write_day(start_date, key, value)

    def delete_redundancy(self, parameter, day=None):
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

    def update_daily_average_temp(self, parameter, day=None):
        '''
        This function reads the recorded temperature values of a day,
        calculates the mean value and stores it in the daily database.
        '''
        logging.info("Calculation mean temperature and writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
        try:
            mean_temp = self.get_mean(parameter, start_date)
            self.write_day(start_date, parameter, mean_temp)
        except Exception as e:
            logging.error("Something went wrong: " + str(e))

    def get_mean(self, parameter, day=None):
        logging.debug("Calculation average for {}".format(parameter))
        start_date, end_date = datevalues.date_values(day)
        res = self.read_day(start_date, parameter)
        res = np.array(res)
        return(round(np.mean(res[:,2]),2))

    def daily_updates(self, day):
        '''
        This method performs the daily updates. Day must be given in format
        "2020-10-10",
        '''
        logging.info("Performing daily database updates")
        self.update_solar_gain(day=day)
        self.update_pellet_consumption(day=day)
        self.update_heating_energy("VerbrauchHeizungEG", day=day)
        self.update_heating_energy("VerbrauchHeizungDG", day=day)
        self.update_heating_energy("VerbrauchWW", day=day)
        self.update_heating_energy("VerbrauchStromEg", day=day)
        self.update_heating_energy("VerbrauchStromOg", day=day)
        self.update_heating_energy("VerbrauchStromAllg", day=day)
        self.delete_redundancy("OekoStorageFill", day=day)
        self.delete_redundancy("OekoStoragePopper", day=day)
        self.delete_redundancy("OekoCiStatus", day=day)
        self.delete_redundancy("OekoPeStatus", day=day)
        self.update_daily_average_temp("OekoAussenTemp", day=day)
        self.update_electrical(day=day)

if __name__ == "__main__":
    '''
    When this file is called as a main program, it is a helper for daily jobs.
    To use this file for testing, just call the file withour arguments.
    For daily jobs, ther are arguments:
        mysqldose.py -d day -u
        -d day: Specifiy day in format "2020-10-10", could also be "yesterday"
        -u: perform daily updates
        e.g.; mysqldose.py -d yesterday -u
    '''
    # Creating object
    dbconn = Mysqldose('heizung', 'heizung', 'dose', 'heizung')
    # Varible initialization
    day = None
    update = False
    #Check if arguments are valid
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'd:u')
    except getopt.GetoptError as err:
        logging.error("Arguments error!")
        exit()
    #Parsing arguments
    for o,a in opts:
        if(o == "-d"):
            if(a == "yesterday"):
                day = datetime.date.today() - datetime.timedelta(1)
                day = day.strftime("%Y-%m-%d")
            else:
                try:
                    day = datetime.datetime.strptime(a, "%Y-%m-%d")
                except:
                    logging.error("Invalid date format. Use something like 2020-10-10. Bye")
                    exit()
        elif(o == "-u"):
            update = True


    if(update):
        dbconn.daily_updates(day)


    #dbconn.influx_query2(day)
    dbconn.influx_calc_energy("2022-02-19")


    #start_date = datetime.date(2018,8,11)
    #day_count = 841
    #for single_date in (start_date + datetime.timedelta(n) for n in range(day_count)):
    #    print(single_date)
    #    dbconn.update_daily_average_temp("OekoAussenTemp", day=single_date)

    #logging.info("Bye.")


    #dbconn.write('2017-11-12 1:2:3', 'Test', 44.0)
    #result = dbconn.read_one("OekoKollLeistung", "2018-09-12")
    #dbconn.update_pellet_consumption(day="2020-10-23")
    #dbconn.update_heating_energy("VerbrauchHeizungEG",day="2020-10-23")
    #print(dbconn.read_many("OekoAussenTemp", "2020-10-18 18:01%"))
    #print(dbconn.read_one("OekoAussenTemp"))
