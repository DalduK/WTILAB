import json
import numpy as np
import pandas as pd

import wtiproj05_redis as rr
import wtiproj04_ETL_and_data_processing as w


class api():
    def get(self):
        ll = rr.get_rand_user()
        print(ll)
        ll = ll.fillna(0)
        return ll.to_dict()

    def post(self, data):
        print(data)
        immutable = frozenset(data.items())
        read_frozen = dict(immutable)
        id = 0
        for k, v in read_frozen.items():
            if v == 0:
                read_frozen[k] = np.nan
            elif k == 'userID':
                id = v
            else:
                read_frozen[k] = float(v)
        rr.add_ocena(read_frozen)
        rr.update_user(id)
        return read_frozen

    def get_all(self):
        r = rr.get_data().to_dict('r')
        print(r)
        jsonfiles = json.dumps(r)
        return jsonfiles

    def delet(self):
        self.df = self.df.iloc[0:0]
        return json.dumps(self.df.to_json(orient='records'))

    def avg_usr(self, user):
        df = rr.get_data()
        list1 = df.columns.values.tolist()
        df.fillna(value=pd.np.nan, inplace=True)
        list1.remove("movieID")
        list1.remove("rating")
        list1.remove("userID")
        id = float(user)
        mean = w.user_genres_mean(df, list1, str(id))
        dict = {}
        for key in range(len(list1)):
            dict[list1[key]] = mean[key]
        dict['userID'] = user
        return dict

    def avg_all(self):
        df = rr.get_data()
        list1 = df.columns.values.tolist()
        df.fillna(value=pd.np.nan, inplace=True)
        list1.remove("movieID")
        list1.remove("rating")
        list1.remove("userID")
        mean, _ = w.mean_genres(df, list1, True)
        dict = {}
        for key in range(len(list1)):
            dict[list1[key]] = mean[key]
        return dict

    def get_profile(self, user):
        return rr.get_user(user)
