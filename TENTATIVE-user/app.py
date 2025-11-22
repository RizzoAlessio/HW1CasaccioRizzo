from flask import Flask, request, jsonify, render_template, redirect, url_for
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
    return render_template("index.html", mail = 0)

@app.route("/register", methods=["POST"])
def register():
    data = request.get_json()
    if not data or "email" not in data:
        return jsonify({"errore": "parametri non presenti"}), 400

    email = data["email"]
    nome = data["nome"]
    cognome = data["cognome"]
    cf = data["cf"]

    try:
        db = get_connection()
        cursor = db.cursor()
        cursor.execute(
            "INSERT INTO utenti (email, nome, cognome, cf) VALUES (%s, %s, %s, %s)",
            (email, nome, cognome, cf)
        )
        db.commit()
    except mysql.connector.IntegrityError:
        return jsonify({"errore": "Email già registrata"}), 409
    except mysql.connector.Error as e:
        return jsonify({"errore di MySQL": str(e)}), 500
    finally:
        cursor.close()
        db.close()

    return render_template("index.html", mail = email)# and jsonify({"OK": "Utente registrato"}), 201
    
@app.route("/delete", methods=["DELETE"])
def delete_user():
    data = request.get_json()
    if not data or "email" not in data:
        return jsonify({"errore": "parametri non presenti"}), 400
    
    email = data["email"]

    try:
        db = get_connection()
        cursor = db.cursor()
        cursor.execute("DELETE FROM utenti WHERE email = %s", (email,))
        db.commit()

        if cursor.rowcount == 0:
            return jsonify({"errore": "utente non trovato"}), 404

    except mysql.connector.Error as e:
        return jsonify({"errore di MySQUL": str(e)}), 500
    finally:
        cursor.close()
        db.close()

    return redirect(url_for("/"))# and jsonify({"message": f"User {email} deleted"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
