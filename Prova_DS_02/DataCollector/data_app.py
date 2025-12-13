from flask import Flask, request, jsonify
import requests, os, time, json, logging
import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
from grpc_client import check_user
from breaker import CircuitBreaker, CircuitBreakerOpenException
from confluent_kafka import Producer, KafkaException

app = Flask(__name__)
logger = logging.getLogger(__name__)
user_data = [""]
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60, expected_exception=Exception)

logging.basicConfig(level=logging.INFO)
K_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
K_TOPIC = os.getenv("KAFKA_TOPIC_ALERT_SYSTEM", "to-alert-system")

producer_config = {
        'bootstrap.servers': K_SERVERS,
        'acks': 'all',
        'linger.ms': 10,
        'retries': 3
    }
producer = None

def get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
    )

def report(err, msg):
    if err is not None:
        logging.error(f"Errore nella consegna: {err}")
    else:
        logging.info(f"Messaggio consegnato a {msg.topic()}")

def send_kafka(icao, count, typ):
    global producer, user_data
    low = high = None
    if producer is None:
        logging.error("Producer Kafka non inizializzato per qualche motivo...")
        return
    try:
        db = get_conn()
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT l, h FROM pref WHERE email=%s AND icao=%s", (user_data[0], icao))
        pref = cursor.fetchone()
        db.commit()
        low = pref.get("l")
        high = pref.get("h")
    except mysql.connector.Error as e:
        logging.error(f"errore di MySQL nel prelevare lh: {str(e)}")
    finally:
        cursor.close()
        db.close()
    logging.info(f"l e h {low} e {high}")
    event = {
        "icao": icao,
        "count": count,
        "user": user_data[0],
        "low": low,
        "high": high,
        "collector": "DataCollector",
        "type": typ
    }
    try:
        producer.produce(
            K_TOPIC,
            json.dumps(event).encode("utf-8"),
            callback=report
        )
        producer.poll(0)
        producer.flush()
    except Exception as e:
        logging.error(f"Errore nel send_kafka {e}")

def add_pref(user, icao, l, h):
    if user == "" or not icao:
        return jsonify({"errore": "parametri non trovati"}), 404
    try:
        db = get_conn()
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT id, l, h FROM pref WHERE email=%s AND icao=%s", (user, icao))
        pref = cursor.fetchone()
        if pref is None:
            cursor.execute("INSERT INTO pref (email, icao, l, h) VALUES (%s, %s, %s, %s)", (user, icao, l, h))
        elif l != 0 and h != 0 and (pref["l"] != l or pref["h"] != h):
            cursor.execute("UPDATE pref SET l = %s, h = %s WHERE email=%s AND icao=%s", (l, h, user, icao))
        db.commit()
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQL in pref": str(e)}), 500
    finally:
        cursor.close()
        db.close()
    return None

def periodic():
    try:
        db = get_conn()
        cursor = db.cursor()
        cursor.execute("SELECT DISTINCT icao FROM pref")
        rows = cursor.fetchall()
        if not rows:
            return {"errore": 404, "nessun iaco registrato": 404}
        now = int(time.time())
        begin = now - 12*3600
        end = now
        for r in rows:
            icao = r["icao"]
            data = fetch_flights(icao, begin, end, "departure")
            if not data:
                continue
            dep_count = len(data)
            for v in data:
                icaoA = v.get("icao24")
                estDep = v.get("estDepartureAirport")
                estArr = v.get("estArrivalAirport")
                departureDate = datetime.fromtimestamp(v.get("firstSeen")).date()
                arrivalDate = datetime.fromtimestamp(v.get("lastSeen")).date()
                cursor.execute("DELETE FROM voli WHERE estDep = %s AND departureDate = %s", (estDep, departureDate))
                cursor.execute("INSERT IGNORE INTO voli (icao24, estDep, estArr, departureDate, arrivalDate) VALUES (%s, %s, %s, %s, %s)",(icaoA, estDep, estArr, departureDate, arrivalDate))
        db.commit()
        send_kafka(icao, dep_count, "periodic departure")
        return {"OK": 200, "aggiornato automaticamente": now}
    except mysql.connector.Error as e:
        return {"errore di MySQL": str(e)}, 500
    finally:
        cursor.close()
        db.close()

def get_token(auth):
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    params = {"grant_type": "client_credentials", "client_id": auth[0], "client_secret": auth[1]}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=params, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

def check_pref(icao, user):
    db = get_conn()
    cursor = db.cursor()
    cursor.execute("SELECT id FROM pref WHERE email=%s AND icao=%s", (user, icao))
    pref = cursor.fetchone()
    cursor.close()
    db.close()
    return pref

def fetch_flights(icao, begin, end, type):
    cred = json.load(open("credentals.json"))
    OPENSKY_USER = cred.get("clientId")
    OPENSKY_SECR = cred.get("clientSecret")
    auth = (OPENSKY_USER, OPENSKY_SECR)
    token = get_token(auth)
    url = f"https://opensky-network.org/api/flights/{type}?airport={icao}&begin={begin}&end={end}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = circuit_breaker.call(requests.get, url, headers=headers, timeout=10)
    except CircuitBreakerOpenException:
        return None     
    except requests.exceptions.RequestException as e:
        return None     
    else:
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 404:
            return []
    return None

########################

@app.route("/<mail>", methods=["POST"])
def verify_user(mail):
    global user_data
    ref = check_user(mail)
    if ref['exists'] == False:
        return jsonify({"errore": "utente non registrato"}), 401
    if user_data[0] == mail:
        return jsonify({"errore": "utente già registrato, proseguire"}), 202
    #if user_data[0] != "" and user_data[0] != mail:
        #return jsonify({"errore": "utente prima di lei stà usando il sistema"}), 202
    user_data[0] = mail
    return jsonify({"OK": 200, "è effettivamente registrato": user_data[0]})

@app.route("/airports", methods=["POST"])
def add_airport():
    if user_data[0] == "":
        return jsonify({"attenzione": "utente deve essere verificato"}), 401
    data = request.json
    icao = data.get("icao")
    name = data.get("name")
    city = data.get("city")
    country = data.get("country")
    low = int(data.get("low", 0))
    high = int(data.get("high", 0))
    if not icao:
        return jsonify({"errore": "ID ICAO mancante"}), 404
    if low != 0 and high != 0 and (low >= high):
        return jsonify({"errore": "parametri low o high sbagliati"}), 416
    try:
        db = get_conn()
        cursor = db.cursor()
        cursor.execute("INSERT INTO aeroporti (icao, name, city, country) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE icao=icao", (icao, name, city, country))
        db.commit()
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQL airport": str(e)}), 500
    finally:
        cursor.close()
        db.close()
    add_pref(user_data[0], icao, low, high)
    return jsonify({"OK": 200, "Aeroporto registrato": icao,})

@app.route("/airports/<icao>/<type>", methods=["GET"])
def add_flight(icao, type):
    if user_data[0] == "":
       return jsonify({"attenzione": "Utente deve essere prima verificato"}), 401
    pref = check_pref(icao, user_data[0])
    if pref is None:
        return jsonify({"attenzione": "Aeroporto non registrato da questo utente"}), 403
    now = int(time.time())
    begin = now - 12*3600
    end = now
    n = 0
    if type == "departure":
        data = fetch_flights(icao, begin, end, "departure")
    if type == "arrival":
        data = fetch_flights(icao, begin, end, "arrival")
    if data is None:
        return jsonify({"errore": "Circuito Aperto"}), 503
    for v in data:
        icaoA = v.get("icao24")
        estDep = v.get("estDepartureAirport")
        estArr = v.get("estArrivalAirport")
        departureDate = datetime.fromtimestamp(v.get("firstSeen")).date()
        arrivalDate = datetime.fromtimestamp(v.get("lastSeen")).date()
        try:
            db = get_conn()
            cursor = db.cursor()
            cursor.execute("INSERT IGNORE INTO voli (icao24, estDep, estArr, departureDate, arrivalDate) VALUES (%s, %s, %s, %s, %s)", (icaoA, estDep, estArr, departureDate, arrivalDate))
            #add_pref(user_data[0], icao)
            n += 1
            db.commit()
        #except mysql.connector.IntegrityError:
            #return jsonify({"errore": "Probabilmente già inserito"}), 409
        except mysql.connector.Error as e:
            return jsonify({"errore di MySQL": str(e)}), 500
        finally:
            cursor.close()
            db.close()
    send_kafka(icao, n, type)     
    return jsonify({"OK": user_data[0], "abbiamo registrato tot aerei": n})

@app.route("/airports/<icao>/7average")
def avg_flights(icao):
    days = 7
    since = (datetime.now().date() - timedelta(days = days - 1)).isoformat()
    db = get_conn()
    cursor = db.cursor(dictionary=True)
    sql = "SELECT departureDate AS day, COUNT(*) AS conto FROM voli WHERE departureDate >= %s AND (estDep = %s OR estArr = %s) GROUP BY departureDate ORDER BY departureDate ASC"
    cursor.execute(sql, (since, icao, icao))
    rows = cursor.fetchall()
    cday = {str(r["day"]): r["conto"] for r in rows}
    daily = []
    for i in range(days):
        day = (datetime.now().date() - timedelta(days = i)).isoformat()
        daily.append({"day": day, "conto": cday.get(day, 0)})
    avg = sum(item["conto"] for item in daily) / days
    return jsonify({"icao": icao, "media": avg, "conteggio": daily})

scheduler = BackgroundScheduler()
scheduler.add_job(periodic, 'interval', hours=12)
scheduler.start()

if __name__ == "__main__":
    logging.basicConfig(filename='datacollector.log', level=logging.INFO)
    while True:
        try:
            producer = Producer(producer_config)
            logging.info("Kafka Producer connesso Data")
            break
        except KafkaException as e:
            logging.info("Kafka non pronto, retry in 5s...")
            time.sleep(5)

    app.run(host="0.0.0.0", port=5001)
