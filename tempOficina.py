import paho.mqtt.client as mqtt
import ssl
import psycopg2
from psycopg2 import extras
import psycopg2.errors
import os
from datetime import datetime
import json

db = os.getenv('POSTGRES_DB', 'postgres')
user = os.getenv('POSTGRES_USER', 'postgres')
password = os.getenv('POSTGRES_PASS', 'password')
host = os.getenv('POSTGRES_HOST','localhost')
port = os.getenv('POSTGRES_PORT', '54321')



def pprint(text):
    now = datetime.now()
    print("-" * 71)
    print("[ " + now.strftime("%d/%m/%Y %H:%M:%S") + " ]  " + text)


def on_connect(client, userdata, flags, rc):
    pprint("Connected with result code " + str(rc))
    client.subscribe("arq", 2)


def on_message(client, userdata, msg):
    pprint("TOPIC " + msg.topic + " | PAYLOAD " + str(msg.payload))
    abrirPuerto(msg.payload)


def asignavar(payload):
    pay = json.loads(payload)
    temp = pay[0]['temperature']
    hum = pay[0]['humidity']
    co2 = pay[0]['CO2']
    return {
        temp, hum, co2
    }


def abrirPuerto(payload):
    con = psycopg2.connect(dbname=db, user=user, password=password, host=host, port=port)
    temp, hum, co2 = asignavar(payload)
    try:

        time_message = datetime.now()
        query = """
                         INSERT INTO public.newtable VALUES
                     ('{}',{},{},{}) 
               """.format(time_message, temp, hum, co2)

        cur = con.cursor()
        cur.execute(query)
        con.commit()
    except Exception as e:
        pprint("No introduce en la base de datos")
    finally:
        con.close()


def main():
    while True:
        try:
            client = mqtt.Client()
            client.on_connect = on_connect
            client.on_message = on_message
            client.username_pw_set("bosonit", "Bosonit2020")
            client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
                           tls_version=ssl.PROTOCOL_TLS, ciphers=None)
            client.connect("rota.elliotcloud.com", 8883, 60)
            client.loop_forever()
        except Exception as e:
            pprint(f"No conectado, {e}")


if __name__ == '__main__':
    main()
