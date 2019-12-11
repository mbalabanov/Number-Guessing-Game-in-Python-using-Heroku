import random
import uuid
from flask import Flask, render_template, request, make_response, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash

from models import User, db

app = Flask(__name__)
db.create_all()  # create (new) tables in the database

@app.route("/", methods=["GET"])
def index():
    session_token = request.cookies.get("session_token")

    if session_token:
        user = db.query(User).filter_by(session_token=session_token).first()
    else:
        user = None

    return render_template("index.html", user=user)

@app.route("/login", methods=["POST"])
def login():
    name = request.form.get("user-name")
    email = request.form.get("user-email")
    password = request.form.get("user-password")

    # create a secret number
    secret_number = random.randint(1, 30)

    # see if user already exists
    user = db.query(User).filter_by(email=email).first()

    if not user:
        # create a User object
        user = User(name=name, email=email, password=generate_password_hash(password), secret_number=secret_number)

        # save the user object into a database
        db.add(user)
        db.commit()

    if not check_password_hash(user.password, password):
        return "Sorry, your password is not correct."

    session_token = str(uuid.uuid4())
    user.session_token = session_token

    db.add(user)
    db.commit()

    # save user's token into a cookie
    response = make_response(redirect(url_for('index')))
    response.set_cookie("session_token", session_token, httponly=True, samesite="Strict")

    return response

@app.route("/logout")
def logout():
        delcookie = make_response(render_template("index.html"))
        delcookie.delete_cookie('email')
        return delcookie

@app.route("/result", methods=["POST"])
def result():
    guess = int(request.form.get("guess"))

    session_token = request.cookies.get("session_token")

    # get user from the database based on her/his email address
    user = db.query(User).filter_by(session_token=session_token).first()

    if guess == user.secret_number:
        message = "Correct! The secret number is {0}".format(str(guess))

        # create a new random secret number
        new_secret = random.randint(1, 30)

        # update the user's secret number
        user.secret_number = new_secret

        # update the user object in a database
        db.add(user)
        db.commit()
    elif guess > user.secret_number:
        message = "Your guess is not correct... try something smaller."
    elif guess < user.secret_number:
        message = "Your guess is not correct... try something bigger."

    return render_template("result.html", message=message)

if __name__ == '__main__':
    app.run(debug=True)
