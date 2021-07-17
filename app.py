from database import Database
from scrapy import Scrapy

from flask import Flask, request, jsonify

CITIES = ['Warsaw', 'Berlin', 'New Delhi']
app = Flask(__name__)


@app.route('/')
def index():
    return """<xmp>
                  /cities -> list of all locations
                  /forecast -> list of the latest forecast for each location for everyday
                  /avgTemp -> list of the average the_tmp of the last 3 forecasts for each location for everyday
                  </xmp>"""


@app.route('/cities', methods=['GET'])
def cities():
    if request.method == 'GET':
        db = Database()

        db.cursor.execute("SELECT city_name FROM cities_tb")
        output = [
            dict(city_name=row[0])
            for row in db.cursor.fetchall()
        ]
        if output is not None:
            del db
            return jsonify(output)


@app.route('/forecast', methods=['GET'])
def forecast():
    if request.method == 'GET':
        db = Database()

        db.cursor.execute('SELECT DISTINCT applicable_date FROM warsaw_tb')
        pom = [x[0] for x in db.cursor.fetchall()]

        output = []

        for c in CITIES:
            for p in pom:
                db.cursor.execute('SELECT * FROM ' + c.replace(" ", "").lower() + "_tb" + ' WHERE applicable_date = "' + p + '" ORDER by created DESC LIMIT 1')

                output_curr = [
                    dict(weather_id=row[0], city_id=row[1], weather_state_name=row[2], weather_state_abbr=row[3], wind_direction_compass=row[4], created=row[5],
                         applicable_date=row[6], min_temp=row[7], max_temp=row[8], the_temp=row[9], wind_speed=row[10], wind_direction=row[11],
                         air_pressure=row[12], humidity=row[13], visibility=row[14], predictability=row[15])
                    for row in db.cursor.fetchall()
                ]

                output.append(output_curr[0])

        if output is not None:
            del db
            return jsonify(output)


@app.route('/avgTemp', methods=['GET'])
def avgTemp():
    if request.method == 'GET':
        db = Database()

        db.cursor.execute('SELECT DISTINCT applicable_date FROM warsaw_tb')
        pom = [x[0] for x in db.cursor.fetchall()]

        output = []

        for c in CITIES:
            for p in pom:
                db.cursor.execute(
                    'SELECT  city_id, AVG(the_temp) AS avgTemp FROM  (SELECT * FROM  ' + c.replace(" ", "").lower() + "_tb" + ' WHERE applicable_date = "' + p + '" ORDER by created LIMIT 3 )')

                output_curr = [
                    dict(date=p, city_id=row[0], avgTemp=row[1])
                    for row in db.cursor.fetchall()
                ]

                output.append(output_curr[0])

        if output is not None:
            return jsonify(output)


def getData():
    scrapy = Scrapy()
    scrapy.downloadData()


def createDb():
    db = Database()
    db.dropTables()
    db.createTables()
    del db


if __name__ == '__main__':
    createDb()
    getData()
    app.run()
