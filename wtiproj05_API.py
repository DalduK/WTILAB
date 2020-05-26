import json
import numpy as np
import pandas as pd

import wtiproj03_ETL



class api():
    df, list1 = wtiproj03_ETL.jjpd()
    df = df.fillna(0)
    def get(self):
        value = self.df.sample(n=1)
        jsonfiles = json.loads(value.to_json(orient='records'))
        return jsonfiles

    def post(self, data):
        dict1 = {}
        columns = []
        for k, v in data.items():
            try:
                dict1[k] = int(v)
                columns.append(k)
            except:
                columns.append(k)
                dict1[k] = float(v)
        dict1.update()
        print(dict1)
        df2 = pd.DataFrame([dict1], columns=dict1.keys())
        self.df = self.df.append(df2, ignore_index=True, verify_integrity=False, sort=None)
        return data

    def get_all(self):
        jsonfiles = json.loads(self.df.to_json(orient='records'))
        return jsonfiles

    def delet(self):
        self.df = self.df.iloc[0:0]
        return json.dumps(self.df.to_json(orient='records'))

    def avg_usr(self, user):
        self.df.replace(0, np.nan, inplace=True)
        mean = wtiproj03_ETL.user_genres_mean(self.df, self.list1, int(user))
        dict = {}
        for key in range(len(self.list1)):
            dict[self.list1[key]] = mean[key]
        dict['userID'] = user
        self.df = self.df.fillna(0)
        return dict

    def avg_all(self):
        self.df.replace(0, np.nan, inplace=True)
        mean, _ = wtiproj03_ETL.mean_genres(self.df, self.list1, True)
        dict = {}
        for key in range(len(self.list1)):
            dict[self.list1[key]] = mean[key]
        return dict