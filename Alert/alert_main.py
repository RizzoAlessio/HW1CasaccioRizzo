import os, json, time, logging
from confluent_kafka import Consumer, Producer, KafkaException

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_IN = os.environ["KAFKA_TOPIC_CONSUME"]
TOPIC_OUT = os.environ["KAFKA_TOPIC_PRODUCE"]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "alert-system-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

producer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "linger.ms": 10,
    "retries": 3,
}

consumer = producer = None


def report(err, msg):
    if err:
        logging.error("Errore nel delivery dell'Alert: %s", err)
    else:
        logging.info("Messaggio alert inviato a %s", msg.topic())


def alert():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error("Errore Kafka: %s", msg.error())
                continue
            event = json.loads(msg.value().decode("utf-8"))
            logging.info("Evento ricevuto: %s", event)
            icao = event["icao"]
            count = event["count"]
            high = event["high"]
            low = event["low"]
            user = event["user"]
            triggered = None
            if high != 0 and count > high:
                triggered = "HIGH"
            elif low != 0 and count < low:
                triggered = "LOW"

            if not triggered:
                consumer.commit(msg)
                continue
            alert_msg = { "email": user, "airport": icao, "type": triggered,}
            producer.produce(
                TOPIC_OUT,
                json.dumps(alert_msg).encode("utf-8"),
                callback=report,
            )
            producer.poll(0)
            consumer.commit(msg)
    except KeyboardInterrupt:
        logging.info("Alert interrotto manualmente")
    finally:
        logging.info("Chiudo Alert")
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    logging.basicConfig(filename='alert.log', level=logging.INFO)
    while True:
        try:
            consumer = Consumer(consumer_conf)
            consumer.subscribe([TOPIC_IN])
            producer = Producer(producer_conf)
            logging.info("Kafka Producer/Consumer connesso Alert")
            break
        except KafkaException:
            logging.warning("Kafka non pronto, retry in 2s...")
            time.sleep(2)
    alert()

