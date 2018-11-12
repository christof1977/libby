#!/usr/bin/env python3

import socket
import sys
import json
import syslog

#global s_udp_sock

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


def sende(udp_socket,addr,port,msg):
    #global s_udp_sock
    #global Ziel
    #global Port
    if(udp_socket == None):
        print("Öffne Socket")
        try:
            udp_socket = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
        except Exception as e:
            print(str(e))
    valid_cmds = getcmds()
    #if msg in valid_cmds:
    if True:
        print("Gewaehlter Eingang:", msg)
        try:
            udp_socket.sendto( msg.encode(), (addr,port) )
            print("Gesendet")
        except Exception as e:
            print("Verbindungsfehler:", str(e))
    else:
        print("Not a valid command!")

def main():
    #addr = '127.0.0.1'
    addr = 'osmd.fritz.box'
    #addr = '192.168.178.37'
    port = 5005

    #win = curses.initscr()
    #curses.cbreak()
    #win.nodelay(True)


    #global s_udp_sock
    s_udp_sock = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
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
                sende(s_udp_sock, addr, port, json_string)
                if cmd == "?":
                    hilf()
                elif cmd == "q":
                    print("Bye")
                    break
            except KeyboardInterrupt:
                print("Bye")
                break
    elif len(sys.argv) == 2:
        if sys.argv[1] in valid_cmds:
            log = "Die Fernbedienung sagt: " + sys.argv[1]
            print(log)
            syslog.syslog(log)
            sende(s_udp_sock, addr, port, sys.argv[1])
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



