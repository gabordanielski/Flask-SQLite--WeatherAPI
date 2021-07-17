from database import Database

import requests
import re


class Scrapy:
    def __init__(self):
        self.woeid = {523920: "Warsaw",
                      638242: "Berlin",
                      28743736: "New Delhi"
                      }

    def downloadData(self):
        for i in [523920, 638242, 28743736]:
            for j in range(1, 8):
                result = requests.get('https://www.metaweather.com/api/location/' + str(i) + '/2021/6/' + str(j) + '/')
                for r in result.json():
                    db = Database()

                    weather_state_name = r['weather_state_name']
                    weather_state_abbr = r['weather_state_abbr']
                    wind_direction_compass = r['wind_direction_compass']

                    # regex for data format normalization
                    dateformatRegex = re.compile('(\d\d\d\d-\d\d-\d\d)T(\d\d:\d\d:\d\d)')
                    searchresult = dateformatRegex.search(r['created'])

                    created = searchresult.group(1) + " " + searchresult.group(2)
                    applicable_date = r['applicable_date']
                    min_temp = r['min_temp']
                    max_temp = r['max_temp']
                    the_temp = r['the_temp']
                    wind_speed = r['wind_speed']
                    wind_direction = r['wind_direction']
                    air_pressure = r['air_pressure']
                    humidity = r['humidity']
                    visibility = r['visibility']
                    predictability = r['predictability']

                    db.cursor.execute("""INSERT INTO """ + self.woeid[i].replace(" ", "").lower() + "_tb" + """ VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
                                   (None, [523920, 638242, 28743736].index(i) + 1, weather_state_name, weather_state_abbr,
                                    wind_direction_compass, created, applicable_date, min_temp, max_temp,
                                    the_temp, wind_speed, wind_direction, air_pressure, humidity, visibility,
                                    predictability))

                    db.connection.commit()
                    del db
