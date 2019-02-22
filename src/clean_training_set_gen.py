import json
import os
import sys


def most_common(lst):
    return max(set(lst), key=lst.count)


def purge_file(filename):
    print("purge %s..." % filename)
    try:
        os.remove(filename)
    except FileNotFoundError:
        print("file not found.")


def process_raw_file(input_data):
    sizes = []
    famacha_array = []
    serial_array = []
    famacha_date_array = []
    animal_data = []
    for line in input_data:
        l = json.loads(line)
        famacha_data_array = l[0]
        sensor_data_array = l[1]
        s = len(sensor_data_array)
        sizes.append(s)

        d = []
        for data in sensor_data_array:
            # cast values to string for array dump
            d.append("%s,%s,%s,%s,%s" % (str(data[0]) if data[0] is not None else 'NaN',  # index
                                         str(data[1]) if data[1] is not None else 'NaN',  # activity
                                         str(data[2]) if data[2] is not None else 'NaN',  # temp
                                         str(data[3]) if data[3] is not None else 'NaN',  # humidity
                                         str(data[4]) if data[4] is not None else 'NaN'))  # prevous_score

        animal_data.append(d)
        famacha_array.append(famacha_data_array[0])
        serial_array.append(famacha_data_array[1])
        famacha_date_array.append(famacha_data_array[2])

    # m_c = most_common(sizes)
    features_count = sizes[0]
    print(features_count, sizes)
    purge_file('count.data')
    with open('count.data', 'a') as outfile_c:
        outfile_c.write(str(features_count))
        outfile_c.close()

    purge_file('training_time_domain.data')
    with open('training_time_domain.data', 'a') as outfile:
        for i in range(0, features_count):
            item = animal_data[i]
            #temp string for debuging
            training_str_formated = ','.join(item) + ',' + str(famacha_array[i]) + ',' + str(
                serial_array[i]) + ',' + str(
                famacha_date_array[i])
            print(training_str_formated[-200:])
            #final string for training set file
            training_str_flatten = ','.join(item) + ',' + str(famacha_array[i])
            outfile.write(training_str_flatten)
            outfile.write('\n')


if __name__ == '__main__':
    print(sys.argv)
    # input raw training data generated with raw_training_set_gen.py
    lines = [line.rstrip('\n') for line in open('raw.json')]
    process_raw_file(lines)