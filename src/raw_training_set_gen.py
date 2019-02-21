import json
import os
import os.path
import sys
import time
from datetime import datetime,timedelta

import pymysql
import tables
import xlrd
from dateutil.relativedelta import *
from ipython_genutils.py3compat import xrange


def purge_file(filename):
    print("purge %s..." % filename)
    try:
        os.remove(filename)
    except FileNotFoundError:
        print("file not found.")


def generate_table_from_xlsx(path):
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_index(1)
    data = {}
    for row_index in xrange(0, sheet.nrows):
        row_values = [sheet.cell(row_index, col_index).value for col_index in xrange(0, sheet.ncols)]
        if row_index == 2:
            time_w = list(filter(None, row_values))
            print(time_w)

        if row_index == 3:
            idx_w = [index for index, value in enumerate(row_values) if value == "WEIGHT"]
            idx_c = [index for index, value in enumerate(row_values) if value == "FAMACHA"]

        chunks = []
        if row_index > 4:
            for i in xrange(0, len(idx_w)):
                if row_values[1] is '':
                    continue
                s = "40101310%s" % row_values[1]
                serial = int(s.split('.')[0])
                chunks.append([time_w[i], row_values[idx_c[i]], serial])
            if len(chunks) != 0:
                data[serial] = chunks
    print(data)
    return data


def get_temp_humidity(date, data):
    humidity = None
    temp = None
    try:
        data_d = data[date.strftime("%Y-%m-%d")]
        time_d = date.strftime("%H:%M:%S").split(':')[0]
        for item in data_d:
            if time_d == item['time'].split(':')[0]:
                humidity = item['humidity']
                temp = item['temp_c']
    except KeyError as e:
        # print(e)
        pass

    return int(0 if temp is None else temp), int(0 if humidity is None else humidity)


def get_previous_famacha_score(serial_number, famacha_test_date, data_famacha, curr_score):
    previous_score = None
    list = data_famacha[serial_number]
    for i in range(1, len(list)):
        item = list[i]
        if item[0] == famacha_test_date:
            try:
                previous_score = int(list[i-1][1])
            except ValueError as e:
                previous_score = curr_score
                # print(e)
            break
    return previous_score


def pad(a, N):
    a += [-1] * (N - len(a))
    return a


def connect_to_sql_database(db_server_name="localhost", db_user="axel", db_password="Mojjo@2015", db_name="south_africa_test4",
                            char_set="utf8mb4", cusror_type=pymysql.cursors.DictCursor):
    # print("connecting to db %s..." % db_name)
    global sql_db
    sql_db = pymysql.connect(host=db_server_name, user=db_user, password=db_password,
                             db=db_name, charset=char_set, cursorclass=cusror_type)
    return sql_db


def execute_sql_query(query, records=None, log_enabled=False):
    try:
        sql_db = connect_to_sql_database()
        cursor = sql_db.cursor()
        if records is not None:
            print("SQL Query: %s" % query, records)
            cursor.executemany(query, records)
        else:
            if log_enabled:
                print("SQL Query: %s" % query)
            cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            if log_enabled:
                print("SQL Answer: %s" % row)
        return rows
    except Exception as e:
        print("Exeception occured:{}".format(e))


def get_elapsed_time_string(time_initial, time_next):
    dt1 = datetime.fromtimestamp(time_initial)
    dt2 = datetime.fromtimestamp(time_next)
    rd = relativedelta(dt2, dt1)
    return '%d years %d months %d days %d hours %d minutes %d seconds' % (
        rd.years, rd.months, rd.days, rd.hours, rd.minutes, rd.seconds)


if __name__ == '__main__':
    print(sys.argv)
    with open(sys.argv[4]) as f:
        weather_data = json.load(f)

    data_famacha = generate_table_from_xlsx(sys.argv[2])
    data_famacha_flattened = [y for x in data_famacha.values() for y in x]

    time_between_tests = []
    for i, current_date in enumerate(data_famacha_flattened[:-1]):
        next_date = data_famacha_flattened[i + 1]
        if current_date[2] == next_date[2]:
            d2 = datetime.strptime(next_date[0], '%d/%m/%Y')
            d1 = datetime.strptime(current_date[0], '%d/%m/%Y')
            diff = (d2 - d1)
            time_between_tests.append(diff.days)
            # print(diff.days, current_date, next_date)
    print("minimum delay between famacha tests is %d days." % min(time_between_tests))

    data_activity = []
    if sys.argv[3] == 'h5':
        h5 = tables.open_file(sys.argv[1], "r")
        data_activity = h5.root.resolution_min.data

    if sys.argv[3] == 'sql':
        db_server_name = "localhost"
        db_user = "axel"
        db_password = "Mojjo@2015"
        cusror_type = pymysql.cursors.DictCursor
        sql_db = pymysql.connect(host=db_server_name, user=db_user, password=db_password)
        connect_to_sql_database()
        farm_id = "Delmas_70101200027"

    purge_file('raw.json')

    print("generating training sets....")
    for i, data_f in enumerate(data_famacha_flattened):
        famacha_test_date = time.strptime(data_f[0], "%d/%m/%Y")
        try:
            famacha_score = int(data_f[1])
        except ValueError as e:
            # print(e)
            continue
        animal_id = data_f[2]

        dates_list = []
        dates_list_formated = []
        activity_list = []
        indexes = []
        idx = 0
        temperature_list = []
        humidity_list = []
        previous_famacha_score_list = []

        #find the activity data of that animal the n days before the test
        N_DAYS = 6
        famacha_test_date_epoch_s = str(time.mktime(famacha_test_date)).split('.')[0]
        famacha_test_date_epoch_before_s = str(time.mktime((datetime.fromtimestamp(time.mktime(famacha_test_date)) -
                                                            timedelta(days=N_DAYS)).timetuple())).split('.')[0]

        data_activity = execute_sql_query("SELECT timestamp, serial_number, first_sensor_value FROM %s_resolution_d"
                                          " WHERE timestamp BETWEEN %s AND %s AND serial_number = %s" %
                                          (farm_id, famacha_test_date_epoch_before_s, famacha_test_date_epoch_s,
                                           str(animal_id)))
        print("mapping activity to famacha score progress=%d/%d ..." % (i, len(data_famacha_flattened)))
        for j, data_a in enumerate(data_activity):
            #transform date in time for comparaison
            curr_datetime = datetime.utcfromtimestamp(int(data_a['timestamp']))
            activity_date = time.strptime(curr_datetime.strftime('%d/%m/%Y'), "%d/%m/%Y")
            # find temp and humidity of the day
            temp, humidity = get_temp_humidity(curr_datetime, weather_data)
            idx += 1
            activity_list.append(data_a['first_sensor_value'])
            indexes.append(idx)
            dates_list.append(activity_date)
            temperature_list.append(temp)
            humidity_list.append(humidity)
            dates_list_formated.append(datetime.utcfromtimestamp(int(data_a['timestamp'])).strftime('%d/%m/%Y %H:%M'))

        previous_famacha_score = get_previous_famacha_score(animal_id, datetime.fromtimestamp(time.mktime(famacha_test_date))
                                                            .strftime("%d/%m/%Y"), data_famacha, famacha_score)
        indexes.reverse()
        debugging_array = [famacha_score, animal_id,
                           time.strftime('%d/%m/%Y', time.localtime(int(famacha_test_date_epoch_s))),
                           time.strftime('%d/%m/%Y', time.localtime(int(famacha_test_date_epoch_before_s)))]
        training_data = [debugging_array, []]

        # fill the training array
        for i in range(0, len(indexes)):
            training_data[1].append([indexes[i], activity_list[i], temperature_list[i], humidity_list[i],
                                     previous_famacha_score])

        # print(len(dates_list_formated), dates_list_formated)
        # print(len(indexes), len(training_data[5]), training_data)

        with open('raw.json', 'a') as outfile:
            json.dump(training_data, outfile)
            outfile.write('\n')
