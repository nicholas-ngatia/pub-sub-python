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
        # Print the received message type and the message itself
        print("Received message type a='{}' b='{}' c='{}' d='{}'".format(type(ex.a), type(ex.b), type(ex.c), type(ex.d)))
        print("Received message a='{}' b='{}' c='{}' d='{}'".format(ex.a, ex.b, ex.c, ex.d))
        consumer.acknowledge(msg)
    except:
        # In the event the message failed to be processed
        consumer.negative_acknowledge(msg)

client.close()
