import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "check-topic",
    "auto.offset.reset": "earliest",   # read from the start of the topic
})
consumer.subscribe(["transactions"])

count = 0
empty_polls = 0
while empty_polls < 5:                 # stop after about 5 seconds with no new messages
    msg = consumer.poll(1.0)
    if msg is None:
        empty_polls += 1
        continue
    if msg.error():
        print("error:", msg.error())
        continue
    empty_polls = 0
    count += 1
    if count <= 3:
        print(json.loads(msg.value()))

consumer.close()
print("total messages read:", count)