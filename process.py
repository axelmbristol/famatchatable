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
    activity_array = []
    famacha_array = []
    serial_array = []
    famacha_date_array = []
    for line in lines:
        l = json.loads(line)
        famacha_score = l[0]
        activity_levels = l[5]
        data = [(x) for x in activity_levels]
        if len(data) == 0:
            continue
        sizes.append(len(data))
        activity_array.append(data)
        famacha_array.append(famacha_score)
        serial_array.append(l[1])
        famacha_date_array.append(l[2])
    m_c = most_common(sizes)
    count = sizes.count(m_c)
    print(m_c, count, sizes)
    purge_file('count.data')
    with open('count.data', 'a') as outfile_c:
        outfile_c.write(str(count))
        outfile_c.close()

    purge_file('training_time_domain_i.data')
    with open('training_time_domain_i.data', 'a') as outfile:
        for i, item in enumerate(activity_array):
            if len(item) == m_c:
                item_c = []
                item_f = []
                for v in item:
                    if v[1] is None:
                        s = str(v[0])+',NaN'
                        item_f.append(str([v[0], None]))
                        item_c.append(s)
                        continue
                    s = str(v[0]) + ',' + str(v[1])
                    item_f.append(str(v))
                    item_c.append(s)

                training_str = ','.join(item_c) + ',' + str(famacha_array[i]) + ',' + str(serial_array[i]) + ',' + str(
                    famacha_date_array[i])
                training_str_formated = ','.join(item_f) + ',' + str(famacha_array[i]) + ',' + str(serial_array[i]) + ',' + str(
                    famacha_date_array[i])
                print(training_str_formated[-200:])
                training_str_flatten = ','.join(item_c) + ',' + str(famacha_array[i])
                if i == 0:
                    print(training_str_flatten[-200:])
                outfile.write(training_str_flatten)
                outfile.write('\n')

