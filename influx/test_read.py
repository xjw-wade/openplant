from client import InfluxDBClient
import time
import json
from datetime import datetime as DT
import pandas as pd
import pickle
from time_trans import *
import math
import numpy as np
# from memory_profiler import profile


# client = InfluxDBClient(host="172.17.86.16", port=8086, username='root', password='root', database='test')
# client = InfluxDBClient(host="192.168.0.106", port=8085, username='root', password='root', database='test')
# client = InfluxDBClient(host="106.55.173.219", port=8085, username='root', password='root', database='test')
# client = InfluxDBClient(host="192.168.31.251", port=8091, username='root', password='root', database='test')
# client = InfluxDBClient(host="192.168.31.251", port=8086, username='root', password='root', database='test')
client = InfluxDBClient(host="172.17.51.169", port=8091, username='root', password='root', database='test')


# @profile
def test_read():
    start_time = Normaltime("2022-11-03 00:00:00")
    s_date = start_time.date()
    s_hour = "%02d" % start_time.hour
    start_time_utc = DT.utcfromtimestamp(float(Changestamp(start_time)))
    s_utc_str = ChangestrUTC(start_time_utc)
    duration = 3600
    time_delta = datetime.timedelta(seconds=duration)
    end_time = Changedatetime(Changestamp(start_time) + duration)
    e_date = end_time.date()
    e_hour = "%02d" % end_time.hour
    end_time_utc = start_time_utc + time_delta
    e_utc_str = ChangestrUTC(end_time_utc)
    sample_step = 1
    tags = []

    with open("keys_test1.txt", "r") as f:
        for line in f.readlines():
            line = line.strip('\n')  # 去掉列表中每一个元素的换行符
            tags.append(line)
    tags_str = "\",\"".join(tags)
    # print(tags_str)
    # query_str = 'SELECT "Time" FROM "sis_data"'
    query_str = 'SELECT \"{}\", "Time" FROM "sis_data" WHERE time >= \'{}\' and time <= \'{}\' and "Time" % {} = 0' .format(tags_str, s_utc_str,e_utc_str, str(sample_step))
    # query_str = 'SELECT first(*) FROM "sis_data" WHERE time >= \'{}\' and time <= \'{}\' GROUP BY time({}s) FILL(previous)'.format(s_utc_str, e_utc_str, sample_step)
    # query_str = 'SELECT first(*) FROM "opc_data" WHERE date = \'{}\' and time >= \'{}\' and time <= \'{}\' GROUP BY time({}s) FILL(previous)'.format(s_date, s_utc_str, e_utc_str, sample_step)
    # query_str = 'SELECT first(*) FROM "opc_data" WHERE date = \'{}\' and hour = \'{}\' and time >= \'{}\' and time <= \'{}\' GROUP BY time({}s) FILL(previous)'.format(s_date, s_hour, s_utc_str, e_utc_str, sample_step)
    # print(query_str)


    t1 = time.time()
    result = client.query(query_str)
    t2 = time.time()
    print('read success')
    # print("Result: {0}".format(result))
    print(t2 - t1)
    data = result._raw['series'][0]['values']
    # print(data)
    columns = result._raw['series'][0]['columns']
    # for index in range(len(columns)):
    #     if (columns[index].startswith('first_')):
    #         columns[index] = columns[index].strip('first_')
    pdata = pd.DataFrame(data, columns=columns)
    print(pdata[0:50])

    # pdata.interpolate(inplace=True)
    # res = []
    # switch_volume = ['DI', 'DO', 'ZSO', 'ZSC', 'ZT', 'FB', 'VO', 'VC', 'ZO', 'ZC', 'MS', 'MD', 'PE', 'PG', 'PL', 'PM', 'ZD']
    # t1 = time.time()
    # for column in columns:
    #     print(column[-2:])
    #     if column[-2:] in switch_volume:
    #         pdata[column].ffill(inplace=True)
    #         pdata[column].bfill(inplace=True)
    #     else:
    #         pdata[column].interpolate(inplace=True)
    # t2 = time.time()
    # # for i, t in enumerate(pdata['Time']):
    # #     if t % 2 == 0:
    # #         res.append(pdata[i:i+1])
    # for i in range(len(pdata)):
    #     if i % 10 == 0:
    #         res.append(pdata[i:i+1].values[0].tolist())
    #         # print(pdata[i:i+1].values[0].tolist())
    # res_data = pd.DataFrame(res, columns=columns)
    # print(res_data[0:20])

    # print(pdata[0:1], type(pdata[0:1]))

    static_points = []
    null_points = []
    
    for col in pdata.columns:
        if col != 'time' and col != 'Time':
            if math.isclose(pdata[col].std(), 0, rel_tol=1e-09, abs_tol=0.0):
            # if pdata[col].std() == 0:
                static_points.append(col)
            if pdata[col].isnull().all():
                null_points.append(col)
    print(len(null_points))
    print(len(static_points))

    # with open('keys_not_exit.txt', 'w') as f:
    #     for point in null_points:
    #         f.write(point)
    #         f.write('\n')



if __name__ == '__main__':
    test_read()

    # print(type(result))

    # path = './test_pickle'

    #
    # print(pdata['Time'])
    # pdata = pdata.fillna(method='bfill')
    #
    # static_points = []
    # null_points = []
    #
    # for col in pdata.columns:
    #     if col != 'time':
    #         if math.isclose(pdata[col].std(), 0, rel_tol=1e-09, abs_tol=0.0):
    #             static_points.append(col)
    #         if pdata[col].isnull().all():
    #             null_points.append(col)


    # null_points = []
    # rdata = pdata.dropna(axis='columns', how='all')
    # notnull = list(rdata.columns)
    # for c in pdata.columns:
    #     if c not in notnull:
    #         null_points.append(c)
    # print(null_points)

    # static_points = []
    # for col in pdata.columns[1:-1]:
    #     print(col)
    #     print(pdata[col], type(pdata[col]))
    #
    #     print(pdata[col].std())
    #     print(pdata[col])
    #     # print(pdata[col].std())
    #     if math.isclose(pdata[col].std(), 0, rel_tol = 1e-09, abs_tol=0.0):
    #         static_points.append(col)
    # print(static_points)
    # print(pdata)
    # print(static_points)
    # print(null_points)
    # print(pdata['N3DCS.3PTMS109S'])


    # print(static_points)

    # null_points = []
    # rdata = pdata.dropna(axis='columns', how='all')
    # notnull = list(rdata.columns)
    # for c in pdata.columns:
    #     if c not in notnull:
    #         null_points.append(c)
    # print(null_points)
    # # print(data[0])
    # # print(type(data), len(data), len(data[0]))
    # # print(data)
    # print(pdata)
    # pdata = pdata.fillna(method='bfill')
    # print(pdata['N4DCS.4ITAP119AI'])
    # print(rdata)
    # df.to_pickle(path)
    # print('write success')

# f1 = open('frame_pickle', 'rb')
# data1 = pickle.load(f1)
# print(data1)
