import os, json, logging, time
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_IN = os.environ["KAFKA_TOPIC_CONSUME"]

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "notify-system-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}


while True:
    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe([TOPIC_IN])
        logging.info("Kafka Consumer connesso Notify")
        break
    except KafkaException:
        logging.info("Kafka Notify non pronto, retry in 2s...")
        time.sleep(2)

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.info("Errore nel kafka notify:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))
        logging.info("NOTIFICA RICEVUTA:", event)

        email = event["email"]
        airport = event["airport"]
        tipo = event["type"]
        logging.info(f"NOTIFY: invio alert a {email} per {airport} ({tipo})")
        consumer.commit(msg)

    except KeyboardInterrupt:
        pass
    finally:
        logging.info("Chiudo consumer Notify")
        consumer.close()