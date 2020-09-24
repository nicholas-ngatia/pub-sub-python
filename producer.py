import pulsar
from pulsar.schema import *


class Data(Record):
    a = Integer()
    b = String()
    c = Float()
    d = Array(String())

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                    topic='data_identify',
                    send_timeout_millis=100000,
                    schema=AvroSchema(Data))


for i in range(100000):
    producer.send(Data(a=i, b=str(i), c=float(i), d=[str(i), str(i+1), str(i+2)]))

client.close()