#!/usr/bin/env python3
# This Python file uses the following encoding: utf-8


import socket
import sys
import json
import select
import logging

ADDR = "heizungeg.local"
PORT = 5005
udpTimeout = 4

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
        logging.debug("Öffne Socket")
        try:
            udpSocket = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
            udpSocket.setblocking(0)
        except Exception as e:
            logging.info(str(e))
    try:
        ret = -1
        logging.debug("So laut des heit: %s", msg)
        ready = select.select([], [udpSocket], [], udpTimeout)
        if(ready[1]):
            udpSocket.sendto( msg.encode(), (addr,port) )
            logging.debug("Gesendet")
        ready = select.select([udpSocket], [], [], udpTimeout)
        if(ready[0]):
            data, addr = udpSocket.recvfrom(8192)
            logging.debug(data.decode())
            ret = json.loads(data.decode())
            return(ret)
    except Exception as e:
        logging.info(str(e))
        return -1



if __name__ == "__main__":
    json_string = '{"command" : "getAlive"}'
    answer = udpRemote(json_string, addr="heizungeg.local", port=5005)
    print(answer)
    print(type(answer))
    pass
