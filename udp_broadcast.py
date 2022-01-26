#!/usr/bin/env python3


import socket
import logging
import json


udpBcPort =  6664


logger = logging.getLogger('udpBroadcast')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)




class udpBroadcast():
    def __init__(self):
        self.udpSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.udpSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT,1)
        self.udpSock.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST, 1)
        self.udpSock.settimeout(0.1)
        logger.info("UDP Broadcast socket created")

    def send(self, message):
        try:
            logger.debug(json.dumps(message))
            self.udpSock.sendto(json.dumps(message).encode(),("<broadcast>",udpBcPort))
        except Exception as e:
            logger.error("Something went wrong while sending UDP broadcast message")
            logger.error(e)
