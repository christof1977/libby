#!/usr/bin/env python3

import socket
import sys
import json
import syslog
import select
if __name__ == "__main__":
    from logger import logger
else:
    from .logger import logger

logging = True


udpTimeout = 4
ADDR = 'osmd.fritz.box'
PORT = 5005

def getcmds():
    valid_cmds = ['CD',
                  'Schneitzlberger',
                  'Portable',
                  'Hilfssherriff',
                  'Bladdnspiela',
                  'Himbeer314',
                  'Hyperion',
                  'Vol_Up',
                  'Vol_Down',
                  'Mute',
                  'DimOled' ]

    return valid_cmds


def hilf():
    print('')
    print('*******************************')
    print('amp_ctrl.py console remote tool')
    #print('Connection::')
    #print('Address=' + addr)
    #print('Port=' + port)
    print('')
    print('Commands:')
    print('c  -> Input CD')
    print('s  -> Input Schneitzlberger')
    print('b  -> Input Bladdnspiela')
    print('p  -> Input Portable')
    print('h  -> Input Hilfssherriff')
    print('3  -> Input Himbeer314')
    print('')
    print('u  -> Volume up')
    print('d  -> Volume down')
    print('m  -> Mute/Unmute')
    print('a  -> Switch Amp on/off')
    print('o  -> OLED on/off')
    print('z  -> Zustand?')
    print('')
    print('k  -> Change Background Color')
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


def sende(msg, **kwargs):
    if('udpSocket' in kwargs):
        udpSocket = kwargs('udpSocket')
    else:
        if('addr' not in kwargs or 'port' not in kwargs):
            logger("Uiui, wohin soll ich mich nur verbinden? Naja, standard halt.",logging)
            addr = ADDR
            port = PORT
        else:
            addr = kwargs('addr')
            port = kwargs('port')
        logger("Öffne Socket",logging)
        try:
            udpSocket = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
            udpSocket.setblocking(0)
        except Exception as e:
            print(str(e))
    valid_cmds = getcmds()
    if True:
        print("Gewaehlter Eingang:", msg)
        ready = select.select([], [udpSocket], [], udpTimeout)
        if(ready[1]):
            udpSocket.sendto( msg.encode(), (addr,port) )
            print("Gesendet")
        ready = select.select([udpSocket], [], [], udpTimeout)
        if(ready[0]):
            data, addr = udpSocket.recvfrom(1024)
            print(data.decode())
        #except Exception as e:
        #    print("Verbindungsfehler:", str(e))
    else:
        print("Not a valid command!")

def main():
    addr = 'osmd.fritz.box'
    port = 5005

    #udpSocket = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
    valid_cmds = getcmds()


    if len(sys.argv) == 1:
        hilf()
        while True:
            try:
                cmd = getch()
                if cmd == "c":
                    json_string = '{"Aktion" : "Input", "Parameter" : "CD"}\n'
                elif cmd == "s":
                    json_string = '{"Aktion" : "Input", "Parameter" : "Schneitzlberger"}\n'
                elif cmd == "p":
                    json_string = '{"Aktion" : "Input", "Parameter" : "Portable"}\n'
                elif cmd == "h":
                    json_string = '{"Aktion" : "Input", "Parameter" : "Hilfssherriff"}\n'
                elif cmd == "b":
                    json_string = '{"Aktion" : "Input", "Parameter" : "Bladdnspiela"}\n'
                elif cmd == "3":
                    json_string = '{"Aktion" : "Input", "Parameter" : "Himbeer314"}\n'
                elif cmd == "k":
                    json_string = '{"Aktion" : "Hyperion", "Parameter" : "-"}\n'
                elif cmd == "u":
                    json_string = '{"Aktion" : "Volume", "Parameter" : "Up"}\n'
                elif cmd == "d":
                    json_string = '{"Aktion" : "Volume", "Parameter" : "Down"}\n'
                elif cmd == "m":
                    json_string = '{"Aktion" : "Volume", "Parameter" : "Mute"}\n'
                elif cmd == "a":
                    json_string = '{"Aktion" : "Switch", "Parameter" : "Power"}\n'
                elif cmd == "o":
                    json_string = '{"Aktion" : "Switch", "Parameter" : "DimOled"}\n'
                elif cmd == "f":
                    json_string = '{"Aktion" : "Input", "Parameter" : "krampf"}\n'
                elif cmd == "z":
                    json_string = '{"Aktion" : "Zustand"}\n'
                if cmd == "?":
                    hilf()
                elif cmd == "q":
                    print("Bye")
                    break
                sende(json_string)
            except KeyboardInterrupt:
                print("Bye")
                break
    elif len(sys.argv) == 2:
        if sys.argv[1] in valid_cmds:
            log = "Die Fernbedienung sagt: " + sys.argv[1]
            print(log)
            syslog.syslog(log)
            sende(sys.argv[1], addr=None, port=None)
            return()
        else:
            log = "Not a valid command"
            print(log)
            syslog.syslog(log)
            return()
    else:
        log = "Not a valid command"
        print(log)
        syslog.syslog(log)
        return()



if __name__ == "__main__":
   main()



