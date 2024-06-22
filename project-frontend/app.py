from datetime import datetime

from flask import Flask, render_template, request

app = Flask(__name__)


def process_location_and_time(latitude, longitude, date, time):

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # This is a placeholder for your backend function

    from time import sleep

    sleep(2)
    song = f"Processed data for Latitude: {latitude}, Longitude: {longitude}, Date: {date}, Time: {time}"
    score = None
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    return song, score


@app.route("/", methods=["GET", "POST"])
def index():
    song, score = None, None
    if request.method == "POST":
        latitude = request.form["latitude"]
        longitude = request.form["longitude"]
        date = request.form["date"]
        time = request.form["time"]
        song, score = process_location_and_time(latitude, longitude, date, time)
        return render_template("index.html", song=song, score=score)
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=False, port=5000)
