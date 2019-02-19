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


if __name__ == '__main__':
    print(sys.argv)
    lines = [line.rstrip('\n') for line in open('raw.json')]
    sizes = []
    # activity_array = []
    # indexes_array = []
    # temp_array = []
    # humidity_array = []
    # previous_famacha_score_array = []
    famacha_array = []
    serial_array = []
    famacha_date_array = []
    animal_data = []
    for line in lines:
        l = json.loads(line)
        famacha_data_array = l[0]
        sensor_data_array = l[1]
        s = len(sensor_data_array)
        if s == 0:
            continue
        sizes.append(s)

        d = []
        for data in sensor_data_array:
            # cast values to string for array dump
            d.append("%s,%s,%s,%s,%s" % (str(data[0]) if data[0] is not None else 'NaN',  # index
                                         str(data[1]) if data[1] is not None else 'NaN',  # activity
                                         str(data[2]) if data[2] is not None else 'NaN',  # temp
                                         str(data[3]) if data[3] is not None else 'NaN',  # humidity
                                         str(data[4]) if data[4] is not None else 'NaN'))  # prevous_score
            # indexes_array.append(data[0])
            # activity_array.append(data[1])
            # temp_array.append(data[2])
            # humidity_array.append(data[3])
            # previous_famacha_score_array.append(data[4])

        animal_data.append(d)

        famacha_array.append(famacha_data_array[0])
        serial_array.append(famacha_data_array[1])
        famacha_date_array.append(famacha_data_array[2])

    m_c = most_common(sizes)
    count = sizes.count(m_c)
    print(m_c, count, sizes)
    purge_file('count.data')
    with open('count.data', 'a') as outfile_c:
        outfile_c.write(str(m_c))
        outfile_c.close()

    purge_file('training_time_domain_i.data')
    with open('training_time_domain_i.data', 'a') as outfile:
        for i in range(0, count):
            if len(animal_data[i]) == m_c:

                item_c = animal_data[i]

                training_str = ','.join(item_c) + ',' + str(famacha_array[i]) + ',' + str(serial_array[i]) + ',' + str(
                    famacha_date_array[i])

                training_str_formated = ','.join(item_c) + ',' + str(famacha_array[i]) + ',' + str(
                    serial_array[i]) + ',' + str(
                    famacha_date_array[i])

                print(training_str_formated[-200:])

                training_str_flatten = ','.join(item_c) + ',' + str(famacha_array[i])
                if i == 0:
                    print(training_str_flatten[-200:])

                outfile.write(training_str_flatten)
                outfile.write('\n')
