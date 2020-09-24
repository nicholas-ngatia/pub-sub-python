import pulsar
from pulsar.schema import *


class Data(Record):
    a = Integer()
    b = String()
    c = Float()
    d = Array(String())

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe(
                  topic='data_identify',
                  subscription_name='read_data',
                  schema=AvroSchema(Data))

while True:
    msg = consumer.receive()
    ex = msg.value()
    try:
        # print(type(msg.data()))
        print("Received message type a='{}' b='{}' c='{}' d='{}'".format(type(ex.a), type(ex.b), type(ex.c), type(ex.d)))
        print("Received message a='{}' b='{}' c='{}' d='{}'".format(ex.a, ex.b, ex.c, ex.d))
        # Acknowledge successful processing of the message
        consumer.acknowledge(msg)
    except:
        # Message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()