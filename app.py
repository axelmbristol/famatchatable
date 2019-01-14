import os
import re
import uuid
from datetime import datetime

import numpy as np
import tables
from cassandra.cluster import Cluster
from ipython_genutils.py3compat import xrange
from tables import *
import os.path
from collections import defaultdict
import dateutil.relativedelta
import time
import os
import glob
import xlrd
import pandas
import sys


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
                serial = "40101310%s" % row_values[1]
                chunks.append([time_w[i], row_values[idx_c[i]]])
            if len(chunks) != 0:
                data[int(serial.split('.')[0])] = chunks
    print(data)
    return data


if __name__ == '__main__':
    print(sys.argv)
    path = sys.argv[1]
    data_f = generate_table_from_xlsx(sys.argv[2])

    h5 = tables.open_file(path, "r")
    data = h5.root.resolution_d.data
    size = len(data)
    list_raw = []
    a = []

    d_ = {}
    d = {}

    for key in data_f.keys():
        for values in data_f[key]:
            d_[values[0]] = [[], []]

    for key in data_f.keys():
        d[key] = d_

    # for key in data_f.keys():
    #     for values in data_f[key]:
    #         d[key][values[0]] = [[], values[1]]
    for index, x in enumerate(data):
        # print("%d%%" % int((index/size)*100))
        ts = int(x['timestamp'])
        date = datetime.utcfromtimestamp(ts).strftime('%d/%m/%Y')
        serial = x['serial_number']
        activity = x['first_sensor_value']
        value = (date, serial, activity)
        # print(value)

        if serial in data_f:
            # print("foud serial number...")
            list = data_f[serial]
            for i in list:
                if i[0] == date:
                    d[serial][date][0].append(activity)
                    d[serial][date][1] = i[1]
                    # d[serial][1] = i[1]
                    # d[serial] = [d[serial], i[1]]
        print(d)
        # if len(a) != 0:
        #     new_trainning_record = [activity, famacha]
        #     print(new_trainning_record)


