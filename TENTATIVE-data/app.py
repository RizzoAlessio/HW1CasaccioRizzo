from flask import Flask, request, jsonify
import requests, os, time, json
import mysql.connector

app = Flask(__name__)

def get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
    )

@app.route("/airports", methods=["POST"])
def add_airport():
    data = request.json
    icao = data.get("icao")
    iata = data.get("iata")
    name = data.get("name")
    city = data.get("city")
    country = data.get("country")

    if not icao:
        return jsonify({"errore": "ID ICAO mancante"}), 400
    
    try:
        db = get_conn()
        cursor = db.cursor()
        cursor.execute("INSERT INTO aeroporti (icao, iata, name, city, country) VALUES (%s, %s, %s, %s, %s)", (icao, iata, name, city, country))
        db.commit()
    except mysql.connector.IntegrityError:
        return jsonify({"errore": "Probabilmente già inserito"}), 409
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQL": str(e)}), 500
    finally:
        cursor.close()
        db.close()
    return jsonify({"OK": 200, "forse è registrato": 1})

def fetch_flights(icao, begin, end, type):
    url = f"https://opensky-network.org/api/flights/{type}"
    params = {"airport": icao, "begin": begin, "end": end}
    cred = json.load(open("credentals.json"))
    OPENSKY_USER = cred.get("clientId")
    OPENSKY_SECR = cred.get("clientSecret")
    auth = (OPENSKY_USER, OPENSKY_SECR)

    resp = requests.get(url, params=params, auth=auth, timeout=30)
    if resp.status_code == 200:
        return "SI"
    if resp.status_code == 404:
        return "Nessun volo"
    return "errore " + str(resp.status_code)
    # dà sempre 401 poiche hanno cambiato API e anche leggendo doc non ho capito benissimo

@app.route("/airports/<icao>/<type>", methods=["GET"])
def test_dep_flight(icao):
    now = int(time.time())
    begin = now - 24*3600
    end = now
    if type == "departure":
        return fetch_flights(icao, begin, end, "departure")
    if type == "departure":
        return fetch_flights(icao, begin, end, "arrival")
    return fetch_flights(icao, begin, end, "both")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)