#!/usr/bin/env python3

import socket
import sys
import json
import select
import logging


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
    print('t  -> get Timer')
    print('')
    print('y  -> Select controller')
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


def tcpRemote(msg, **kwargs):
    # Todo
    pass


def udpRemote(msg, **kwargs):
    # Setzt einen JSON-String in Richtung Ampi per UDP ab
    # msg: JSON-String nach Ampi-Spec
    # udpSocket: wenn da ein Socket übergeben wird, wird halt der hergenommen
    # addr, port: Gibt's keinen Socket, wird einer mit addr, port aufgemacht
    # Wenn nix übergeben wird, gibt's halt einen Standard-Socket

    if('udpSocket' in kwargs):
        udpSocket = kwargs.get('udpSocket')
    else:
        if('addr' not in kwargs or 'port' not in kwargs):
            logging.info("Uiui, wohin soll ich mich nur verbinden? Naja, standard halt.")
            addr = ADDR
            port = PORT
        else:
            addr = kwargs.get('addr')
            port = kwargs.get('port')
        logging.info("Öffne Socket")
        try:
            udpSocket = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
            udpSocket.setblocking(0)
        except Exception as e:
            logging.info(str(e))
    valid_cmds = getcmds()
    try:
        ret = -1
        logging.info("So laut des heit: %s", msg)
        ready = select.select([], [udpSocket], [], udpTimeout)
        if(ready[1]):
            udpSocket.sendto( msg.encode(), (addr,port) )
            logging.info("Gesendet")
        ready = select.select([udpSocket], [], [], udpTimeout)
        if(ready[0]):
            data, addr = udpSocket.recvfrom(1024)
            logging.info(data.decode())
            ret = data.decode()
            return(ret)
    except Exception as e:
        logging.info(str(e))
        return -1

def get_room(addr):
    ret = json.loads(udpRemote('{"command":"getStatus"}\n', addr=addr, port=5005))
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

def auswahl():
    print('o -> oben')
    print('u -> unten')
    auswahl =  getch()
    if(auswahl == "o"):
        addr = "fbhdg"
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
                elif cmd == "m":
                    room = get_room(addr)
                    json_string = '{"command" : "getRoomMode", "Room" : "'+ room +'"}\n'
                elif cmd == "n":
                    room = get_room(addr)
                    mode = get_mode()
                    json_string = '{"command" : "setRoomMode", "Room" : "'+ room +'", "Mode": "'+ mode +'"}\n'
                elif cmd == "y":
                    addr = auswahl()
                    json_string = '{"command" : "getAlive"}\n'
                elif cmd == "o":
                    room = get_room(addr)
                    json_string = '{"command" : "getRoomShortTimer", "Room" : "' + room + '"}\n'
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
                            print(json.dumps(json.loads(ret),indent=4))
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



