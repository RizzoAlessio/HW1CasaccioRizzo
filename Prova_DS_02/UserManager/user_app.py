from flask import Flask, request, jsonify
import mysql.connector
import os

app = Flask(__name__)

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

@app.route("/")
def index():
    return jsonify({"ciao": "test"}), 200

@app.route("/register", methods=["POST"])
def register():

    key = request.headers.get("Request-Key")
    data = request.get_json()
    status = 200
    if not key:
        return jsonify({"errore": "Manca la chiave"}), 404

    try:
        db = get_connection()
        cursor = db.cursor()

        cursor.execute("SELECT _status FROM richiest WHERE request_key = %s", (key,))
        resp = cursor.fetchone()
        if resp == 200:
            return jsonify({"errore", "Rischiesta già processata"})
        try:
            if not data or "email" not in data:
                status = 404
            else:
                email = data["email"]
                nome = data["nome"]
                cognome = data["cognome"]
                cf = data["cf"]
                cursor.execute("INSERT INTO utenti (email, nome, cognome, cf) VALUES (%s, %s, %s, %s)", (email, nome, cognome, cf))
        except mysql.connector.IntegrityError:
            status = 501
        except mysql.connector.Error as e:
            status = 500
            return jsonify({"STATUS": e})
        cursor.execute("INSERT INTO richiest (request_key, _status) VALUES (%s, %s)", (key, status))
        db.commit()
        return jsonify({"STATUS": status})
    except mysql.connector.Error as e:
        return jsonify({"è successo un errore": str(e)})
    finally:
        cursor.close()
        db.close()
        

@app.route("/delete", methods=["DELETE"])
def delete_user():
    data = request.get_json()
    if not data or "email" not in data:
        return jsonify({"errore": "parametri non presenti"}), 404
    email = data["email"]
    try:
        db = get_connection()
        cursor = db.cursor()
        cursor.execute("DELETE FROM utenti WHERE email = %s", (email,))
        #cursor.execute("DELETE FROM datadatabase.pref WHERE email = %s", (email,))
        db.commit()
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQUL": str(e)}), 500
    finally:
        cursor.close()
        db.close()
    return jsonify({"ok": "Utente e sue occorrenze eliminate"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
