import hashlib
from joblib import load
import pandas as pd


def getHash(item):
    item = str(item)
    item = hashlib.sha224(item.encode("utf-8")).hexdigest()
    hash = int(item, 16)
    return str(hash % 10000)


class api:
    def __init__(self):
        self.model = load("/home/dldk/PycharmProjects/WTILAB/diabetes_clf.joblib")
        self.predictions = {}

    def getPrediction(self, value):
        hashedValue = getHash(value)
        if hashedValue in self.predictions:
            pred = self.predictions[hashedValue]
        else:
            pred = self.addPrediction(value)
            self.predictions[hashedValue] = pred
        return pred, hashedValue

    def addPrediction(self, value):
        newDf = {}
        for key in value:
            newDf[key] = [value[key]]
        df = pd.DataFrame(newDf)
        item = self.model.predict_proba(df)[0][0]
        return item

    def updateModel(self, filename):
        self.model = load("/home/dldk/PycharmProjects/WTILAB/"+filename+".joblib")
        self.predictions = {}
        return "The model " + filename + " has been loaded sucessfully!"

if __name__ == '__main__':
    print(getHash('100'))