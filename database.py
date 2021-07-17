import sqlite3


class Database:
    def __init__(self):
        self.connection = sqlite3.connect('weather.db')
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.close()

    def createTables(self):
        self.cursor.execute("""CREATE TABLE if not exists cities_tb(
                            city_id INTEGER PRIMARY KEY,
                            city_name text NOT NULL
                            )""")

        for city in ['Warsaw', 'Berlin', 'New Delhi']:
            self.cursor.execute("""INSERT INTO cities_tb VALUES (?,?);""", (None, city))
            self.cursor.execute("""create table if not exists """ + city.replace(" ", "").lower() + "_tb" + """(
                                    weather_id INTEGER PRIMARY KEY,
                                    city_id integer,
                                    weather_state_name text,
                                    weather_state_abbr text,
                                    wind_direction_compass text,
                                    created text,
                                    applicable_date text,
                                    min_temp real,
                                    max_temp real,
                                    the_temp real,
                                    wind_speed real,
                                    wind_direction real,
                                    air_pressure real,
                                    humidity integer,
                                    visibility real,
                                    predictability integer,
                                    foreign key (city_id)
                                        references cities (city_id)
                                    )""")
            self.connection.commit()

    def dropTables(self):
        self.cursor.execute("""DROP table IF EXISTS cities_tb""")
        self.cursor.execute("""DROP table IF EXISTS warsaw_tb""")
        self.cursor.execute("""DROP table IF EXISTS berlin_tb""")
        self.cursor.execute("""DROP table IF EXISTS newdelhi_tb""")
