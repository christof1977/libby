#!/usr/bin/env python3

import socket
import sys
import json
import select
import logging

from remote import udpRemote

#logging.basicConfig(level=logging.DEBUG)

udpTimeout = 4
ADDR = 'heizungeg'
PORT = 5005

def getcmds():
    valid_cmds = ['getAlive',
                  'getStatus',
                  'getRooms',
                  'getRoomStatus',
                  'setRoomStatus',
                  'getTimer',
                  'setTimer' ]

    return valid_cmds


def hilf():
    print('')
    print('*******************************')
    print('heizung console remote tool')
    print('')
    print('Commands:')
    print('a  -> getAlive')
    print('s  -> get Status')
    print('r  -> get Rooms')
    print('e  -> get Room Status')
    print('m  -> get Room Mode')
    print('n  -> set Room Mode')
    print('o  -> get Room Shorttimer')
    print('p  -> set Room Shorttimer')
    print('v  -> get Room normTemp')
    print('b  -> set Room normTemp')
    print('t  -> get Timer')
    print('f  -> reload Timer File')
    print('')
    print('y  -> Select controller')
    print('c  -> Get Counter Values')
    print('w  -> Get Counter')
    print('?  -> This Text')
    print('q  -> Quit')

def getch():
    import sys, tty, termios
    fd = sys.stdin.fileno( )
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch

def get_counter(addr):
    counters = udpRemote('{"command":"getCounter"}\n', addr=addr, port=5005)
    try:
        print("")
        i = 0
        counters = counters["Counter"]
        print(counters)
        for counter in counters:
            print(i, " -> ", counter)
            i += 1
        return(counters[int(getch())])
    except:
        print("Error")

def get_room(addr):
    ret = udpRemote('{"command":"getStatus"}\n', addr=addr, port=5005)
    rooms_avail = {}
    for room in ret:
        rooms_avail[room[0].lower()] = room
        print('')
    for short in rooms_avail:
        print(short, "->", rooms_avail[short])
    room = getch()
    if room in rooms_avail.keys():
        #print(rooms_avail[room])
        return(rooms_avail[room])
    else:
        return("WZ")

def get_mode():
    print('')
    print('0 -> aus')
    print('1 -> an')
    print('a -> auto')
    mode = getch()
    if(mode == "0"):
        return("off")
    elif(mode == "1"):
        return("on")
    else:
        return("auto")

def get_temp():
    temp = float(input("Temp: "))
    return(temp)


def auswahl():
    print('o -> oben')
    print('u -> unten')
    print('g -> piesler')
    auswahl =  getch()
    if(auswahl == "o"):
        addr = "fbhdg"
    elif(auswahl == "g"):
        addr = "piesler"
    else:
        addr = "heizungeg"
    return(addr)

def main():
    addr = 'heizungeg'
    port = 5005

    valid_cmds = getcmds()


    if len(sys.argv) == 1:
        hilf()
        while True:
            try:
                cmd = getch()
                valid = 1
                if cmd == "a":
                    json_string = '{"command" : "getAlive"}\n'
                elif cmd == "s":
                    json_string = '{"command" : "getStatus"}\n'
                elif cmd == "r":
                    json_string = '{"command" : "getRooms"}\n'
                elif cmd == "e":
                    room = get_room(addr)
                    json_string = '{"command" : "getRoomStatus", "Room" : "'+ room +'"}\n'
                elif cmd == "t":
                    room = get_room(addr)
                    json_string = '{"command" : "getTimer", "Room" : "'+ room +'"}\n'
                elif cmd == "f":
                    json_string = '{"command" : "reloadTimer"}'
                elif cmd == "m":
                    room = get_room(addr)
                    json_string = '{"command" : "getRoomMode", "Room" : "'+ room +'"}\n'
                elif cmd == "n":
                    room = get_room(addr)
                    mode = get_mode()
                    json_string = '{"command" : "setRoomMode", "Room" : "'+ room +'", "Mode": "'+ mode +'"}\n'
                elif cmd == "v":
                    room = get_room(addr)
                    json_string = '{"command" : "getRoomNormTemp", "Room" : "'+ room +'"}\n'
                elif cmd == "b":
                    room = get_room(addr)
                    temp = get_temp()
                    json_string = '{"command" : "setRoomNormTemp", "Room" : "'+ room +'", "normTemp": "'+ str(temp) +'"}\n'
                elif cmd == "y":
                    addr = auswahl()
                    json_string = '{"command" : "getAlive"}\n'
                elif cmd == "o":
                    room = get_room(addr)
                    json_string = '{"command" : "getRoomShortTimer", "Room" : "' + room + '"}\n'
                elif cmd == "z":
                    json_string = '{"command" : "reloadTimer"}'
                elif cmd == "c":
                    counter = get_counter(addr)
                    try:
                        json_string = '{"command" : "getCounterValues", "Counter" : "' + counter + '"}'
                    except Exception as e:
                        print("Falsch!")
                elif cmd == "w":
                    json_string = '{"command" : "getCounter"}'
                elif cmd == "p":
                    room = get_room(addr)
                    mode = get_mode()
                    try:
                        seconds = int(input("Minuten: ")) * 60
                        json_string = '{"command" : "setRoomShortTimer", "Room" : "' + room + '", "Mode": "' + mode +'" ,"Time" : "' + str(seconds) + '" }\n'
                    except Exception as e:
                        print("Falsch!", e)
                        break
                elif cmd == "?":
                    hilf()
                    valid = 0
                elif cmd == "q":
                    logging.info("Bye")
                    break
                else:
                    logging.info("Invalid command")
                    valid = 0
                if valid:
                    #ret = udpRemote(json_string, addr="heizungeg", port=5005)
                    ret = udpRemote(json_string, addr=addr, port=5005)
                    if(ret!=-1):
                        try:
                            print(json.dumps(ret,indent=4))
                            #print(ret)
                        except:
                            print("ups")
            except KeyboardInterrupt:
                logging.info("Bye")
                break
    else:
        log = "Not a valid command"
        logging.info(log)
        syslog.syslog(log)
        return()



if __name__ == "__main__":
   main()



