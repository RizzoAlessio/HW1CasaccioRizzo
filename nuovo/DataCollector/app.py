from flask import Flask, request, jsonify
import requests, os, time, json
import mysql.connector
#from DataCollector.grpc_client import check_user

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import sys
sys.path.append("/proto")

app = Flask(__name__)
user = ""

def get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
    )

def add_pref(user, icao):
    if user == "" or not icao:
        return jsonify({"errore": "parametri non trovati"}), 409
    try:
        db = get_conn()
        cursor = db.cursor(dictionary=True)
        cursor.execute( "SELECT id FROM pref WHERE email=%s AND icao=%s", (user, icao))
        pref = cursor.fetchone()
        if pref is None:
            cursor.execute( "INSERT INTO pref (email, icao) VALUES (%s, %s)", (user, icao))
            db.commit()
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQL in pref": str(e)}), 500
    finally:
        cursor.close()
        db.close()
    return None

@app.route("/<mail>", methods=["POST"])
def verify_user(mail):
    #if not check_user(mail):
        #return jsonify({"errore": "utente non registrato"})
    global user
    user = mail
    return jsonify({"OK": 200, "è effettivamente registrato": user})

@app.route("/airports", methods=["POST"])
def add_airport():
    if user == "":
        return jsonify({"attenzione": "utente deve essere verificato"}), 402
    
    data = request.json
    icao = data.get("icao")
    name = data.get("name")
    city = data.get("city")
    country = data.get("country")

    if not icao:
        return jsonify({"errore": "ID ICAO mancante"}), 400
    add_pref(user, icao)
    try:
        db = get_conn()
        cursor = db.cursor()
        cursor.execute("INSERT INTO aeroporti (icao, name, city, country) VALUES (%s, %s, %s, %s)", (icao, name, city, country))
        db.commit()
    except mysql.connector.IntegrityError:
        return jsonify({"errore": "Probabilmente già inserito"}), 409
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQL airport": str(e)}), 500
    finally:
        cursor.close()
        db.close()
    return jsonify({"OK": 200, "forse è registrato": 1})

def get_token(auth):
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    params = {"grant_type": "client_credentials", "client_id": auth[0], "client_secret": auth[1]}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=params, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_flights(icao, begin, end, type):
    cred = json.load(open("credentals.json"))
    OPENSKY_USER = cred.get("clientId")
    OPENSKY_SECR = cred.get("clientSecret")
    auth = (OPENSKY_USER, OPENSKY_SECR)
    token = get_token(auth)
    url = f"https://opensky-network.org/api/flights/{type}?airport={icao}&begin={begin}&end={end}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        return jsonify({"NO": 404, "fNessun volo": 1})
    return jsonify({"errore": 0, "è successo": str(resp.status_code)})

@app.route("/airports/<icao>/<type>", methods=["GET"])
def add_flight(icao, type):
    #"""
    #if user == "":
       #return jsonify({"attenzione": "utente deve essere prima verificato"}), 402
    #try:
        #db = get_conn()
        #cursor = db.cursor()
        #cursor.execute("SELECT id FROM pref WHERE icao=(%s) AND email=(%s)", (icao, user))
        #db.commit()
    #except mysql.connector.IntegrityError:
        #return jsonify({"errore": "Aeroporto non favorito"}), 409
    #except mysql.connector.Error as e:
        #return jsonify({"errore di MySQL": str(e)}), 500
    #finally:
        #cursor.close()
        #db.close()
    # """
    now = int(time.time())
    begin = now - 12*3600
    end = now
    if type == "departure":
        data = fetch_flights(icao, begin, end, "departure")
    if type == "arrival":
        data = fetch_flights(icao, begin, end, "arrival")

    v = data[0]  #per ora il primo
    icaoA = v.get("icao24")
    estDep = v.get("estDepartureAirport")
    estArr = v.get("estArrivalAirport")
    departureDate = datetime.fromtimestamp(v.get("firstSeen")).date()
    arrivalDate = datetime.fromtimestamp(v.get("lastSeen")).date()
    #callsign = data.get("callsign")

    try:
        db = get_conn()
        cursor = db.cursor()
        cursor.execute("INSERT INTO voli (icao24, estDep, estArr, departureDate, arrivalDate) VALUES (%s, %s, %s, %s, %s)", (icaoA, estDep, estArr, departureDate, arrivalDate))
        add_pref(user, icao)
        db.commit()
    except mysql.connector.IntegrityError:
        return jsonify({"errore": "Probabilmente già inserito"}), 409
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQL": str(e)}), 500
    finally:
        cursor.close()
        db.close()
    return ({"OK": icaoA, "è effettivamente registrato": user})


@app.route("/airports/<icao>/7average")
def avg_flights(icao):
    days = int(request.args.get("days", 7))
    since = (datetime.now().date() - timedelta(days=days - 1)).isoformat()
    date_field = "departureDate"
    db = get_conn()
    cursor = db.cursor(dictionary=True)
    sql = f""" SELECT {date_field} AS day, COUNT(*) AS cnt FROM voli WHERE {date_field} >= %s AND {date_field} IS NOT NULL AND (estDep = %s OR estArr = %s) GROUP BY {date_field} ORDER BY {date_field} ASC """
    #sql = f""" SELECT departureDate AS day, COUNT(*) AS cnt FROM voli WHERE departureDate >= %s AND departureDate IS NOT NULL AND (estDep = %s OR estArr = %s) GROUP BY departureDate ORDER BY departureDate ASC """
    cursor.execute(sql, (since, icao, icao))
    rows = cursor.fetchall()

    cday = {str(r["day"]): r["cnt"] for r in rows}
    daily = []
    for i in range(days):
        day = (datetime.now().date() - timedelta(days=i)).isoformat()
        daily.append({"day": day, "cnt": cday.get(day, 0)})

    avg = sum(item["cnt"] for item in daily) / days

    return jsonify({
        "icao": icao,
        "giorni": days,
        "media": avg,
        "conteggio": daily
    })


#scheduler = BackgroundScheduler()
#scheduler.add_job(periodic_collect, 'interval', hours=12)  # ogni 12 ore
#scheduler.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)