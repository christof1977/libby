#!/usr/bin/env python3

#!/usr/bin/python
# -*- coding: utf-8 -*-

# 1-Wire Slave-Liste lesen

class onewires():
    pass

    def enumerate(self):
        self.w1_slaves = []
        fhd = open('/sys/devices/w1_bus_master1/w1_master_slaves')
        w1_slaves = fhd.readlines()
        for i in range(len(w1_slaves)):
            self.w1_slaves.append(w1_slaves[i].split("\n")[0])
        fhd.close()
        return self.w1_slaves

    def getValue(self, w1_slave):
        fhd = open('/sys/bus/w1/devices/' + str(w1_slave) + '/w1_slave')
        filecontent = fhd.read()
        fhd.close()

        # Temperaturwerte auslesen und konvertieren
        stringvalue = filecontent.split("\n")[1].split(" ")[9]
        temperature = float(stringvalue[2:]) / 1000

        # Temperatur ausgeben
        return temperature



    def getAllValues(self):
        # Fuer jeden 1-Wire Slave aktuelle Temperatur ausgeben
        for line in self.w1_slaves:
            temperature = self.getValue(line)
            print(str(line) + ': %6.2f °C' % temperature)

def main():
    ow = onewires()
    w1_slaves = ow.enumerate()
    print(w1_slaves)
    print(type(w1_slaves[0]))
    #ow.getValue(w1_slaves[0])
    import time
    #print(ow.getValue("28-0416c13d4bff"))
    while True:
        try:
            ow.getAllValues()
            time.sleep(1)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
