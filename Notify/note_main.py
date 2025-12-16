import os, json, logging, time
import smtplib
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_IN = os.environ["KAFKA_TOPIC_CONSUME"]

logging.basicConfig(
    filename="notify.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "notify-system-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = None

SMTP_HOST = os.environ.get("SMTP_HOST", "localhost")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 2525))
FROM_EMAIL = os.environ.get("FROM_EMAIL", "notifyvoli@example.com")

def send_email(email, subject, body):
    msg = EmailMessage()
    msg["From"] = FROM_EMAIL
    msg["To"] = email
    msg["Subject"] = subject
    msg.set_content(body)
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.send_message(msg)
        logging.info("Email inviata a %s", email)
    except Exception as e:
        logging.error("Errore invio email %s", str(e))

def notify():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error("Errore Kafka Notify: %s", msg.error())
                continue
            event = json.loads(msg.value().decode("utf-8"))
            logging.info("NOTIFICA RICEVUTA: %s", event)
            email = event["email"]
            airport = event["airport"]
            tipo = event["type"]
            logging.info( "NOTIFY: invio alert a %s per %s (%s)", email, airport, tipo,)
            subject = f"Alert {airport}"
            body = f"Alert per l'aeroporto {airport}. La soglia {tipo} è stata superata."
            send_email(email, subject, body)
            consumer.commit(msg)
    except KeyboardInterrupt:
        logging.info("Notify interrotto manualmente")
    finally:
        logging.info("Chiudo consumer Notify")
        consumer.close()

if __name__ == "__main__":
    logging.basicConfig(filename='note.log', level=logging.INFO)
    while True:
        try:
            consumer = Consumer(consumer_conf)
            consumer.subscribe([TOPIC_IN])
            logging.info("Kafka Consumer connesso Notify")
            break
        except KafkaException:
            logging.warning("Kafka non pronto, retry in 2s...")
            time.sleep(2)
    notify()


