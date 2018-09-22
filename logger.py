import syslog

def logger(msg, logging=True):
    if logging == True:
        print(msg)
        syslog.syslog(str(msg))
