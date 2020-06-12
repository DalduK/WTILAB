import json
import numpy as np
import pandas as pd

import wtiproj03_ETL



class api():
    def get(self):
        return wtiproj03_ETL.get_rand_user().replace(0, np.nan, inplace=True)

    def post(self, data):
        wtiproj03_ETL.add_ocena(data)
        id = data["userId"]
        wtiproj03_ETL.update_user(id)
        return

    def get_all(self):
        jsonfiles = json.dumps(wtiproj03_ETL.get_data().to_dict('index'))
        return jsonfiles

    def delet(self):
        self.df = self.df.iloc[0:0]
        return json.dumps(self.df.to_json(orient='records'))

    def avg_usr(self, user):
        df = wtiproj03_ETL.get_data()
        self.df.replace(0, np.nan, inplace=True)
        mean = wtiproj03_ETL.user_genres_mean(self.df, self.list1, int(user))
        dict = {}
        for key in range(len(self.list1)):
            dict[self.list1[key]] = mean[key]
        dict['userID'] = user
        self.df = self.df.fillna(0)
        return dict

    def avg_all(self):
        df= wtiproj03_ETL.get_data()
        self.df.replace(0, np.nan, inplace=True)
        mean, _ = wtiproj03_ETL.mean_genres(self.df, self.list1, True)
        dict = {}
        for key in range(len(self.list1)):
            dict[self.list1[key]] = mean[key]
        return dict

    def get_profile(self,user):
        return wtiproj03_ETL.get_user(user)
