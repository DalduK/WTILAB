import fakejson
import json
import que
import time
import pandas as pd


def producer(name):
    i = 0
    q = que.Que()
    while i < 1000:
        fjson = fakejson.fakeJson()
        js = json.dumps(fjson)
        q.put(name, js)
        i += 1
        time.sleep(0.01)


def panproducer(name):
    q = que.Que()
    # df = pd.DataFrame(columns=['userID','movieID','rating','date_day','date_month','date_year','date_hour','date_minute','date_second'])
    # f = open('/home/przemysaw/PycharmProjects/wtiproj01/user_ratedmovies.dat',"r")
    df = pd.read_csv('/home/przemysaw/PycharmProjects/wtiproj01/user_ratedmovies.dat', sep=" ", header=None, nrows=100,
                     delimiter="\t",
                     names=['userID', 'movieID', 'rating', 'date_day', 'date_month', 'date_year', 'date_hour',
                            'date_minute',
                            'date_second'])
    # f1 = f.readlines()
    # i=0
    # for x in f1:
    #     if i <1000:
    #         l = x.split()
    #         df = df.append({'userID':l[0],'movieID':l[1],'rating':l[2],'date_day':l[3],'date_month':l[4],'date_year':l[5],'date_hour':l[6],'date_minute':l[7],'date_second':l[8]},ignore_index=True)
    #         i+=1
    # # for row in df.to_dict(orient='record'):
    # #     js = json.dumps(row)
    # #     q.put(name,js)
    # # <--Alternatywa metoda do iterrows -->
    for index, row in df.iterrows():
        print(row)
        js = json.dumps(row.to_dict())
        q.put(name, js)
        time.sleep(1.00 / 4)
