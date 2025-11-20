from flask import Flask, request, jsonify, render_template, redirect, url_for
from flask_mysqldb import MySQL
import traceback
import os

app = Flask(__name__)

app.config['MYSQL_HOST'] = os.getenv("DB_HOST", "user-db")
app.config['MYSQL_DB'] = os.getenv("DB_NAME", "users_db")
app.config['MYSQL_USER'] = os.getenv("DB_USER", "user")
app.config['MYSQL_PASSWORD'] = os.getenv("DB_PASS", "password")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", 5002))
app.config['MYSQL_PORT'] = LISTEN_PORT
mysql = MySQL(app)

def get_userdb_cur():
    try:
        cur = mysql.connection.cursor()
        return cur
    except Exception as e:
        print("ERRORE CONNESSIONE DB:", repr(e))
        traceback.print_exc()
        raise

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/login", methods=["POST"])
def registrazione():

    data = request.json
    if not data or "email" not in data:
        return jsonify({"ERRORE": "Dati utente mancanti"}), 400
    
    email = data.get("email")
    nome = data.get("nome")
    cognome = data.get("cognome")
    cf = data.get("cf")

    try:
        cur = get_userdb_cur()
        cur.execute("INSERT INTO utenti(email, nome, cognome, cf) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY IGNORE", (email, nome, cognome, cf))
        mysql.connection.commit()
        cur.close()
        return render_template("index.html", uno = "OK")
    except Exception as e:
        print("ERRORE REGISTRAZIONE:", repr(e))
        traceback.print_exc()
        return render_template("index.html", uno = repr(e))

@app.route("/user/<email>", methods=["DELETE"])
def cancella_utente(email):
    try:
        cur = get_userdb_cur()
        cur.execute("DELETE FROM utenti WHERE email = %s", (email,))
        mysql.connection.commit()
        cur.close()
        return render_template("index.html", uno = "ELIMINATO")

    except Exception as e:
        print("ERRORE ELIMINAZIONE:", repr(e))
        traceback.print_exc()
        return jsonify({"message": "Errore durante l'eliminazione"}), 500
    return render_template("index.html", uno = "ELIMINATION-NOT-OK")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=True)
