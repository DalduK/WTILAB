import json
import secrets
import wtiproj04_ETL_and_data_processing as wt
import pandas as pd
import redis

r = redis.Redis(host='localhost', port=32768, db=0)
r2 = redis.Redis(host='localhost', port=32768, db=1)


def load_data():
    r2.flushdb()
    df, lista = wt.jjpd()
    print(df)
    print(df.shape)
    lista.insert(0, 'movieID')
    lista.insert(1, 'rating')
    for index, row in df.iterrows():
        r2.rpush(row['userID'], row[lista].to_json())
    print("completed")


def get_data():
    lista = []
    for x in r2.keys():
        x2 = r2.lrange(x.decode("utf-8"), 0, -1)
        for l in x2:
            y = json.loads(l.decode("utf-8"))
            y["userID"] = x.decode("utf-8")
            lista.append(y)
    df = pd.DataFrame(lista)
    return df


def load_users():
    r.flushdb()
    df = get_data()
    df.fillna(value=pd.np.nan, inplace=True)
    lista = df.columns.values.tolist()
    lista.remove("movieID")
    lista.remove("rating")
    lista.remove("userID")
    for i in df.userID.unique():
        usr = wt.user_profile(df, lista, i)
        for h in range(len(lista)):
            r.hset(i, lista[h], usr[h])
    print("completed")


def update_user(id):
    df = get_data()
    lista = df.columns.values.tolist()
    df.fillna(value=pd.np.nan, inplace=True)
    lista.remove("movieID")
    lista.remove("rating")
    lista.remove("userID")
    usr = wt.user_profile(df, lista, id)
    for h in range(len(lista)):
        r.hset(id, lista[h], usr[h])
    print("completed")


def get_users():
    lista = []
    for i in r.keys():
        x2 = r.hgetall(i.decode("utf-8"))
        y = {k.decode("utf-8"): v.decode("utf-8") for k, v in x2.items()}
        y["userID"] = i.decode("utf-8")
        lista.append(y)
    n = json.dumps(lista)
    df = pd.read_json(n)
    return df


def add_ocena(df):
    userId = df['userID']
    del df["userID"]
    de = json.dumps(df)
    r2.rpush(userId, de)


def get_user(id):
    x = r.hgetall(id)
    y = {k.decode("utf-8"): v.decode("utf-8") for k, v in x.items()}
    y["userID"] = id
    return y


def get_ddata(id):
    x2 = r2.lrange(id, 0, -1)
    l = secrets.choice(x2)
    y = json.loads(l.decode("utf-8"))
    y["userID"] = id
    df = pd.DataFrame.from_dict(y, orient='index')
    df.fillna(value=pd.np.nan, inplace=True)
    return df


def get_rand_user():
    x = r2.randomkey()
    return get_ddata(x.decode("utf-8"))
