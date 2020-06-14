
import json
import numpy as np
import pandas as pd

import wtiproj03_ETL as w
import wtiproj04_ETL_and_data_processing as ww


class api():
    df, list1 = w.jjpd()
    df = df.fillna(0)
    def get(self):
        value = self.df.sample(n=1)
        jsonfiles = json.dumps(value.to_dict('index'))
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
        jsonfiles = json.dumps(self.df.to_dict('index'))
        return jsonfiles

    def delet(self):
        self.df = self.df.iloc[0:0]
        return json.dumps(self.df.to_json(orient='records'))

    def avg_usr(self, user):
        df1 = self.df.sample(n=1)
        self.df.replace(0, np.nan, inplace=True)
        mean = ww.user_mean(df1, self.list1, int(user))
        dict = {}
        for key in range(len(self.list1)):
            dict[self.list1[key]] = mean[key]
        dict['userID'] = user
        self.df = self.df.fillna(0)
        return dict

    def avg_all(self):
        self.df.replace(0, np.nan, inplace=True)
        mean, _ = ww.mean_genres(self.df, self.list1, True)
        dict = {}
        for key in range(len(self.list1)):
            dict[self.list1[key]] = mean[key]
        return dict