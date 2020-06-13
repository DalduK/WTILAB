import json
import numpy as np
import pandas as pd

import wtiproj03_ETL



class api():
    def get(self):
        ll = wtiproj03_ETL.get_rand_user()
        print(ll)
        ll = ll.fillna(0)
        return ll.to_dict()

    def post(self, data):
        id = 0
        for k, v in data.items():
            if v == 0:
                data[k] = np.nan
            elif k == 'userID':
                id = v
            else:
                data[k] = float(v)
        wtiproj03_ETL.add_ocena(data)
        wtiproj03_ETL.update_user(id)
        return data

    def get_all(self):
        jsonfiles = json.dumps(wtiproj03_ETL.get_data().to_dict('index'))
        return jsonfiles

    def delet(self):
        self.df = self.df.iloc[0:0]
        return json.dumps(self.df.to_json(orient='records'))

    def avg_usr(self, user):
        df = wtiproj03_ETL.get_data()
        list1 = df.columns.values.tolist()
        list1.remove("movieID")
        list1.remove("rating")
        list1.remove("userID")
        id = float(user)
        mean = wtiproj03_ETL.user_genres_mean(df, list1, str(id))
        dict = {}
        for key in range(len(list1)):
            dict[list1[key]] = mean[key]
        dict['userID'] = user
        return dict

    def avg_all(self):
        df = wtiproj03_ETL.get_data()
        list1 = df.columns.values.tolist()
        list1.remove("movieID")
        list1.remove("rating")
        list1.remove("userID")
        mean, _ = wtiproj03_ETL.mean_genres(df, list1, True)
        dict = {}
        for key in range(len(list1)):
            dict[list1[key]] = mean[key]
        return dict

    def get_profile(self,user):
        return wtiproj03_ETL.get_user(user)
