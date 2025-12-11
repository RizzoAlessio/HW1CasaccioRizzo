import os, json, logging, time
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_IN = os.environ["KAFKA_TOPIC_CONSUME"]

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "notify-system-group",
    "auto.offset.reset": "earliest"
}


while True:
    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe([TOPIC_IN])
        logging.info("Kafka Producer/Consumer connesso Alert")
        break
    except KafkaException as e:
        logging.info("Kafka non pronto, retry in 2s...")
        time.sleep(2)

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Errore nel kafka notify:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))
        print("NOTIFICA RICEVUTA:", event)

        email = event["email"]
        airport = event["airport"]
        tipo = event["type"]
        print(f" → NOTIFY: invio alert a {email} per {airport} ({tipo})")

    except KafkaException as e:
        print("Errore Kafka ij Notify:", e)