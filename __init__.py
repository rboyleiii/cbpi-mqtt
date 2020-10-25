# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt
from eventlet import Queue
from modules import cbpi, app, ActorBase
from modules.core.hardware import SensorActive
import json
import os
import re
import threading
import time
import logging
from modules.core.props import Property

from datetime import datetime, timedelta

cbpi.MQTTActor_Compressors = []

q = Queue()

def on_connect(client, userdata, flags, rc):
    if rc!=0:
        print("Failed to connect mqtt, response: " + str(rc))
    else:
        print("Connected to MQTT server")
    for key, value in cbpi.cache.get("sensors").iteritems():
        if value.type == "MQTT_SENSOR":
            cbpi.cache["mqtt"].client.subscribe(value.config['a_topic'])
            print("(Re)subscribed to: " + value.config['a_topic'])


class MQTTThread (threading.Thread):

    def __init__(self, server, port, tls):
        threading.Thread.__init__(self)
        self.server = server
        self.port = port
        self.tls = tls

    client = None

    def run(self):
        self.client = mqtt.Client()
        self.client.on_connect = on_connect

        if self.tls.lower() == 'true':
            self.client.tls_set_context(context=None)

        try:
            self.client.connect(str(self.server), int(self.port), 60)
        except Exception as e:
            print "Error connecting to MQTT server ("+ str(e)+"), will keep trying.."
        self.client.loop_forever()


@cbpi.actor
class MQTTActor(ActorBase):
    topic = Property.Text("Topic", configurable=True,
                          default_value="", description="MQTT Topic")
    pPower = 100

    def init(self):
        super(MQTTActor, self).init()
        print "Initially setting MQTT Actor to off.."
        self.off()

    def on(self, power):
        if power is not None:
            if power != self.pPower:
                power = min(100, power)
                power = max(0, power)
                self.pPower = int(power)
        #self.api.cache["mqtt"].client.publish(self.topic, payload=json.dumps(
        #    {"on": True, "power": self.pPower}), qos=2, retain=True)
        self.api.cache["mqtt"].client.publish(self.topic, "on", qos=2, retain=True)

    def off(self):
        self.api.cache["mqtt"].client.publish(self.topic, "off", qos=2, retain=True)

    def set_power(self, power):
        self.on(power)

@cbpi.actor
class MQTTActor_Compressor(ActorBase):
    topic = Property.Text("Topic", configurable=True,
                          default_value="", description="MQTT TOPIC")
    delay = Property.Number(
        "Compressor Delay", configurable=True, default_value=10, description="minutes")
    compressor_on = False
    compressor_wait = datetime.utcnow()
    delayed = False

    def init(self):
        super(MQTTActor_Compressor, self).init()
        cbpi.MQTTActor_Compressors += [self]
        print "Initially setting MQTT Actor Compressor to off.."
        self.off()

    def on(self, power=100):
        if datetime.utcnow() >= self.compressor_wait:
            self.compressor_on = True
            self.api.cache["mqtt"].client.publish(self.topic, "on", qos=2, retain=True)
            self.delayed = False
        else:
            cbpi.app.logger.info("Delaying Turing on Compressor")
            self.delayed = True

    def off(self):
        if self.compressor_on:
            self.compressor_on = False
            self.compressor_wait = datetime.utcnow() + timedelta(minutes=int(self.delay))
        self.delayed = False
        self.api.cache["mqtt"].client.publish(self.topic, "off", qos=2, retain=True)

@cbpi.sensor
class MQTT_SENSOR(SensorActive):
    a_topic = Property.Text("Topic", configurable=True,
                            default_value="", description="MQTT Topic")
    b_payload = Property.Text("Sensor ID", configurable=True, default_value="",
                              description="ID of the DS18B20 sensor from Tasmota")
    #c_unit = Property.Text("Unit", configurable=True,
    #                       default_value="", description="Not used anymore, need to remove")
    d_offset = Property.Number("Offset", configurable=True,
                               default_value="0", description="Offset relative to sensor data")

    last_value = None
    send_value = 0

    def init(self):
        self.topic = self.a_topic

        if self.b_payload == "":
            self.payload_text = None
        else:
            self.payload_text = self.b_payload.split('.')
        #self.unit = self.c_unit[0:3]

        SensorActive.init(self)
        def on_message(client, userdata, msg):

            try:
                #print "payload " + msg.payload
                sensorUnits=""

                json_data = json.loads(msg.payload)
                val = json_data

                #build dict of sensors returned in mqtt (up to 8 in tasmota)
                sensorDict={}
                for key in val:
                    if 'DS18B20-' in key:
                        mydict= val[key]
                        sensorDict[str(mydict["Id"])] = str(mydict["Temperature"])
                    if 'TempUnit' in key:
                        sensorUnits=val[key]

                #loop through defined sensors and see if any match mqtt Dict built above
                for key, value in cbpi.cache.get("sensors").iteritems():
                    if value.type == "MQTT_SENSOR":
                        for DSid in sensorDict:
                            if value.config["b_payload"] == DSid:
                                ConvTemp=0
                                if self.get_config_parameter("unit", "C") == sensorUnits:
                                    ConvTemp=(round(float(sensorDict[str(DSid)]) + float(value.config["d_offset"]), 2))
                                    self.unit="C"
                                else:
                                    ConvTemp=(round(9.0 / 5.0 * float(sensorDict[str(DSid)]) + 32 + float(value.config["d_offset"]), 2))
                                    self.unit="F"

                                q.put({"id": value.id, "value": ConvTemp})
                        
                #print("------Original ----")
                #if self.payload_text is not None:

                #    for key in self.payload_text:
                #        val = val.get(key, None)
  
                #if isinstance(val, (int, float, basestring)):
                #    q.put({"id": on_message.sensorid, "value": val})
                #print("------Orig end-----")    

            except Exception as e:
                print e

        on_message.sensorid = self.id
        
        #print("self.id=" + str(self.id))
        #print("Failed to connect mqtt, response: " + str(rc))

        self.api.cache["mqtt"].client.subscribe(self.topic)
        self.api.cache["mqtt"].client.message_callback_add(self.topic, on_message)

    def get_value(self):
        try:
            self.send_value = round(float(self.last_value), 2)
        except Exception as e:
            pass
        return {"value": self.send_value, "unit": self.get_config_parameter("unit", "C")}

    def get_unit(self):
        return self.get_config_parameter("unit", "C")

    def stop(self):
        self.api.cache["mqtt"].client.unsubscribe(self.topic)
        SensorActive.stop(self)

    def execute(self):
        '''
        Active sensor has to handle his own loop
        :return:
        '''
        self.sleep(5)


@cbpi.initalizer(order=0)
def initMQTT(app):

    server = app.get_config_parameter("MQTT_SERVER", None)
    if server is None:
        server = "localhost"
        cbpi.add_config_parameter("MQTT_SERVER", "localhost", "text", "MQTT Server")

    port = app.get_config_parameter("MQTT_PORT", None)
    if port is None:
        port = "1883"
        cbpi.add_config_parameter("MQTT_PORT", "1883", "text", "MQTT Sever Port")

    tls = app.get_config_parameter("MQTT_TLS", None)
    if tls is None:
        tls = "false"
        cbpi.add_config_parameter("MQTT_TLS", "false", "text", "MQTT TLS")

    app.cache["mqtt"] = MQTTThread(server, port, tls)
    app.cache["mqtt"].daemon = True
    app.cache["mqtt"].start()

    def mqtt_reader(api):
        while True:
            try:
                m = q.get(timeout=0.1)
                api.cache.get("sensors")[m.get("id")].instance.last_value = m.get("value")
                api.receive_sensor_value(m.get("id"), m.get("value"))
            except:
                pass

    cbpi.socketio.start_background_task(target=mqtt_reader, api=app)


@cbpi.backgroundtask(key="update_MQTTActor_compressors", interval=5)
def update_MQTTActor_compressors(api):
    for compressor in cbpi.MQTTActor_Compressors:
        if compressor.delayed and datetime.utcnow() >= compressor.compressor_wait:
            compressor.on()
            
