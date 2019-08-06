#!/usr/bin/env python3
from __future__ import print_function
import serial
import stat
import os

try:
    import meterbus
except ImportError:
    import sys
    sys.path.append('../')
    import meterbus



class mbus():

    def __init__(self, **kwargs):
        self.device = kwargs.pop('device',  "/dev/ttyAMA0")
        self.address = kwargs.pop('address', 254)
        self.baudrate = kwargs.pop('baudrate', 2400)
        #meterbus.debug(args.d)
        mode = os.stat(self.device).st_mode

    def ping_address(self, ser, address, retries=5):
        for i in range(0, retries + 1):
            meterbus.send_ping_frame(ser, address)
            try:
                frame = meterbus.load(meterbus.recv_frame(ser, 1))
                if isinstance(frame, meterbus.TelegramACK):
                    return True
            except meterbus.MBusFrameDecodeError:
                pass

        return False

    def do_reg_file(args):
        with open(args.device, 'rb') as f:
            frame = meterbus.load(f.read())
            if frame is not None:
                print(frame.to_JSON())

    def do_char_dev(self, **kwargs):
        address = self.address
        device = self.device
        baudrate = self.baudrate


        try:
            address = int(address)
            if not (0 <= address <= 254):
                address = address
        except ValueError:
            address = address.upper()

        try:
            ibt = meterbus.inter_byte_timeout(baudrate)
            with serial.serial_for_url(device,
                               baudrate, 8, 'E', 1,
                               inter_byte_timeout=ibt,
                               timeout=1) as ser:
                frame = None

                if meterbus.is_primary_address(address):
                    self.ping_address(ser, meterbus.ADDRESS_NETWORK_LAYER, 0)

                    meterbus.send_request_frame(ser, address)
                    frame = meterbus.load(
                        meterbus.recv_frame(ser, meterbus.FRAME_DATA_LENGTH))

                elif meterbus.is_secondary_address(address):
                    meterbus.send_select_frame(ser, address)
                    try:
                        frame = meterbus.load(meterbus.recv_frame(ser, 1))
                    except meterbus.MBusFrameDecodeError as e:
                        frame = e.value

                    assert isinstance(frame, meterbus.TelegramACK)

                    frame = None
                    # ping_address(ser, meterbus.ADDRESS_NETWORK_LAYER, 0)

                    meterbus.send_request_frame(
                        ser, meterbus.ADDRESS_NETWORK_LAYER)

                    time.sleep(0.3)

                    frame = meterbus.load(
                        meterbus.recv_frame(ser, meterbus.FRAME_DATA_LENGTH))

                if frame is not None:
                    #print(frame.to_JSON())
                    return frame.to_JSON()

        except serial.serialutil.SerialException as e:
            print(e)



def main():
    #mb = mbus(device="/dev/ttyAMA0", address=254, baudrate=2400)
    mb = mbus()
    result = mb.do_char_dev()
    print(result)
    pass

if __name__ == '__main__':
    main()
