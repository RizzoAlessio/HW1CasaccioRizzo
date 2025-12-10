import os, json, time, logging
from confluent_kafka import Consumer, Producer, KafkaException

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_IN = os.environ["KAFKA_TOPIC_CONSUME"]
TOPIC_OUT = os.environ["KAFKA_TOPIC_PRODUCE"]

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "alert-system-group",
    "auto.offset.reset": "earliest"
}

producer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "linger.ms": 10,
    "retries": 3
}

while True:
    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe([TOPIC_IN])
        producer = Producer(producer_conf)
        logging.info("Kafka Producer/Consumer connesso Alert")
        break
    except KafkaException as e:
        logging.info("Kafka non pronto, retry in 2s...")
        time.sleep(2)


def report(err, msg):
    if err is not None:
        print("Errore nel delivery dell'Alert:", err)
    else:
        print(f"Messaggio alert inviato a {msg.topic()}")

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("errore kafka:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))

        icao = event["icao"]
        timestamp = event["timestamp"]
        count = event["departure_count"]
        high = event["high"]
        low = event["low"]
        user = event["user"]

        triggered = None
        if high and count > high:
            triggered = "HIGH"
        if low and count < low:
            triggered = "LOW"

        if triggered:
            alert_msg = {
                "email": user,
                "airport": icao,
                "type": triggered
            }

            producer.produce(
                TOPIC_OUT,
                json.dumps(alert_msg).encode("utf-8"),
                callback=report
            )
            producer.poll(0)

    except KafkaException as e:
        print("Errore Kafka:", e)
