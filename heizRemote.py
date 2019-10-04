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
    print('t  -> get Timer')
    print('')
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

def get_room():
    print('')
    print('w -> Wohnzimmer')
    print('s -> Schlafzimmer')
    print('a -> Arbeitszimmer')
    print('k -> Küche')
    print('b -> Bad')
    room = getch()
    rooms = {"w":"WZ",
             "s":"SZ",
             "a":"AZ",
             "k":"K",
             "b":"BadEG"}
    if room in rooms.keys():
        print(rooms[room])
        return(rooms[room])
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


def main():
    addr = 'heizung'
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
                    room = get_room()
                    json_string = '{"command" : "getRoomStatus", "room" : "'+ room +'"}\n'
                elif cmd == "t":
                    room = get_room()
                    json_string = '{"command" : "getTimer", "room" : "'+ room +'"}\n'
                elif cmd == "m":
                    room = get_room()
                    json_string = '{"command" : "getRoomMode", "room" : "'+ room +'"}\n'
                elif cmd == "n":
                    room = get_room()
                    mode = get_mode()
                    json_string = '{"command" : "setRoomMode", "room" : "'+ room +'", "mode": "'+ mode +'"}\n'
                elif cmd == "?":
                    hilf()
                elif cmd == "q":
                    logging.info("Bye")
                    break
                else:
                    logging.info("Invalid command")
                    valid = 0
                if valid:
                    ret = udpRemote(json_string, addr="heizungeg", port=5005)
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



