#!/usr/bin/env python3

import datetime

def date_values(day=None):
    '''
    Returns a datetime object for given day or today with time 00:00:00 as
    start_date and time 23:59:50 as end_date
    '''
    if(day is None):
        start_date = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
    elif(isinstance(day, datetime.date)):
        start_date = datetime.datetime.combine(day, datetime.datetime.min.time())
    else:
        try:
            start_date = datetime.datetime.strptime(day, "%Y-%m-%d")
        except:
            logging.error("Not a valid date")
            return(-1)
    end_date = start_date + datetime.timedelta(hours=23, minutes=59, seconds=59)
    return(start_date, end_date)

def date_values_influx(day=None):
    '''
    Returns a datetime string for given day or today with time 00:00:00 as
    start_date and time 23:59:50 as end_date in format 2021-12-12T00:00:00Z
    for use with influxdb
    '''
    start_date, end_date = date_values(day)
    start_date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    return(start_date, end_date)
