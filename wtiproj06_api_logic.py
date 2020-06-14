import json
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster

import wtiproj06_cassandra_client as cc
import wtiproj04_ETL_and_data_processing as w

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()
keyspace = "user_ratings"
table = "user_avg_rating"
keyspace2 = "usr_profiles"
table2 = "profiles"
class api():
    def get(self):
        ll = cc.get_random_user(session)
        for k,v in ll.items():
            if np.isnan(v):
                ll[k] = 0
        return ll

    def post(self, data):
        print(data)
        immutable = frozenset(data.items())
        read_frozen = dict(immutable)
        id = 0
        for k, v in read_frozen.items():
            if v == 0:
                read_frozen[k] = np.nan
            elif k == 'userid':
                id = v
            else:
                read_frozen[k] = float(v)
        df = pd.DataFrame.from_dict(read_frozen,'index')
        cc.push_data_table2(session,keyspace,table,df)
        cc.push_usr_table(session)
        return read_frozen

    def get_all(self):
        r = cc.get_data_table(session,keyspace,table)
        r.fillna(0)
        dc = r.to_dict('r')
        jsonfiles = json.dumps(dc)
        return jsonfiles

    def delet(self):
        cc.clear_table(session, keyspace, table)
        cc.clear_table(session, keyspace2, table2)
        return json.dumps('')

    def avg_usr(self, user):
        df = cc.get_data_table(session,keyspace,table)
        list1 = df.columns.values.tolist()
        df.fillna(value=pd.np.nan, inplace=True)
        list1.remove("movieid")
        list1.remove("rating")
        list1.remove("userid")
        id = float(user)
        mean = w.user_mean2(df, list1, id)
        dict = {}
        for key in range(len(list1)):
            dict[list1[key]] = mean[key]
        dict['userid'] = user
        return dict

    def avg_all(self):
        df = cc.get_data_table(session,keyspace,table)
        list1 = df.columns.values.tolist()
        df.fillna(value=pd.np.nan, inplace=True)
        list1.remove("movieid")
        list1.remove("rating")
        list1.remove("userid")
        mean, _ = w.mean_genres(df, list1, True)
        dict = {}
        for key in range(len(list1)):
            dict[list1[key]] = mean[key]
        return dict

    def get_profile(self, user):
        return cc.get_usr_table(session, user)

if __name__ == '__main__':
    n = api()
    print(n.get())
    n.get_all()