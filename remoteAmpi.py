#!/usr/bin/env python3
#coding: utf8

import socket
import getopt
import sys
import json
import select
import logging
from remote import udpRemote


logging.basicConfig(level=logging.INFO)

udpTimeout = 4
port = 5005

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
    print('v  -> Activate/Deactivate Amp Output')
    print('w  -> Activate/Deactivate Headphone Output')
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

def sendcmd(cmd):
    valid = 1
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
    elif cmd == "v":
        json_string = '{"Aktion" : "Output", "Parameter" : "AmpOut"}\n'
    elif cmd == "w":
        json_string = '{"Aktion" : "Output", "Parameter" : "HeadOut"}\n'
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
    else:
        valid = 0
    if valid == 1:
        udpRemote(json_string, addr="127.0.0.1", port=port)
    else:
        logging.warning("Invalid command")

def interactive():
    valid_cmds = getcmds()
    if len(sys.argv) == 1:
        hilf()
        while True:
            try:
                cmd = getch()
                if cmd == "?":
                    hilf()
                elif cmd == "q":
                    logging.info("Bye")
                    break
                sendcmd(cmd)
            except KeyboardInterrupt:
                logging.info("Bye")
                break
    else:
        log = "Not a valid command"
        logging.warning(log)
        return()


if __name__ == "__main__":
    argv = sys.argv[1:]
    if(len(argv)==0):
        interactive()
    try:
        opts, args = getopt.getopt(argv, 'i:v:s:', ["input=", "volume=", "switch="])
    except getopt.GetoptError as err:
        logging.error("Arguments error!")
        exit()
    #Parsing arguments
    for o,a in opts:
        if o in ("-i", "--input"):
            if a in ("CD", "cd"):
                sendcmd("c")
            elif a in ("LP", "lp", "Phono", "phono", "Bladdnspiela", "bladdnspiela"):
                sendcmd("b")
            elif a in ("Himbeer314", "himbeer314", "pi", "Pi"):
                sendcmd("3")
            elif a in ("Schneitzlberger", "schneitzlberger", "TV", "tv"):
                sendcmd("s")
            elif a in ("AUX", "Aux", "aux"):
                sendcmd("h")
            elif a in ("Portable", "portable"):
                sendcmd("p")
            else:
                logging.warning("Not a valid input")
        elif o in ("-v", "--volume"):
            if a in ("mute", "Mute"):
                sendcmd("m")
            elif a in ("down", "Down"):
                sendcmd("d")
            elif a in ("up", "Up"):
                sendcmd("u")
            else:
                logging.warning("Not a valid volume control")
        elif o in ("-s", "--switch"):
            if a in ("power", "Power"):
                sendcmd("a")
            elif a in ("mediacenter", "Mediacenter"):
                #tbd
                pass
            elif a in ("dimoled", "DimOled"):
                sendcmd("o")
            else:
                logging.warning("Not a valid switch command")
        else:
            pass



