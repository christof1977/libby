#!/usr/bin/env python3

#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging


logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)


# 1-Wire Slave-Liste lesen

class onewires():

    def __init__(self):
        self.old_values = {}

    def enumerate(self):
        self.w1_slaves = []
        fhd = open('/sys/devices/w1_bus_master1/w1_master_slaves')
        w1_slaves = fhd.readlines()
        for i in range(len(w1_slaves)):
            self.w1_slaves.append(w1_slaves[i].split("\n")[0])
        fhd.close()
        return self.w1_slaves

    def getValue(self, w1_slave):
        def _get_value(w1_slave):
            fhd = open('/sys/bus/w1/devices/' + str(w1_slave) + '/w1_slave')
            filecontent = fhd.read()
            fhd.close()

            # Temperaturwerte auslesen und konvertieren
            try:
                stringvalue = filecontent.split("\n")[1].split(" ")[9]
                ret = float(stringvalue[2:]) / 1000
            except Exception as e:
                ret = 150
            return ret


        temperature = _get_value(w1_slave)

        try:
            # Wenn bereits ein vorhergehender Wert für diesen Sensor gespeichert ist, wird alt und neu verglichen.
            # Wenn die Abweichung größer 5°C ist, gehen wir mal davon aus, dass der ausgelesene Wert falsch ist
            # und lesen ihe einfach nochmal aus, so oft bis die Abweichung kleiner 5°C ist.
            if(abs(temperature-self.old_values[str(w1_slave)]) <= 5):
                # Temperaturwert für nächstes Runde speichern
                self.old_values[str(w1_slave)] = temperature
            else:
                logger.info("Alter Wert: " + self.old_values[str(w1_slave)] + "°C")
                logger.info("NICHT Innerhalb +/- 5 Grad: " + str(temperature) + "°C")
                temperature = _get_value(w1_slave)
        except:
            # Hier geht's rein, wenn der für den Sensor nooch kein alter Wert existiert
            # Temperaturwert für nächstes Runde speichern
            self.old_values[str(w1_slave)] = temperature
        # Temperatur ausgeben
        return temperature

    def getAllValues(self):
        # Fuer jeden 1-Wire Slave aktuelle Temperatur ausgeben
        for line in self.w1_slaves:
            temperature = self.getValue(line)
            logger.info(str(line) + ': %6.2f °C' % temperature)

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
