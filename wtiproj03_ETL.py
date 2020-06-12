import ast
import json
import secrets
import time
from random import random

import pandas as pd
import numpy as np
import redis
import string

r = redis.Redis(host='localhost', port=32768, db=0)
r2 = redis.Redis(host='localhost', port=32768, db=1)

df = pd.read_csv('/home/dldk/PycharmProjects/WTILAB/user_ratedmovies.dat', nrows=100000, delimiter="\t",
                 usecols=["userID", "movieID", "rating"])
df1 = pd.read_csv('/home/dldk/PycharmProjects/WTILAB/movie_genres.dat',
                  delimiter="\t",
                  usecols=["movieID", "genre"])


def jjpd():
    list1 = []
    pd.set_option('expand_frame_repr', False)
    df1["value"] = 1
    df2 = df1.pivot_table(index="movieID", columns="genre", values="value", fill_value=np.nan)
    for column_name in df2.columns:
        list1.append("genre-" + column_name)
        df2.rename(columns={column_name: "genre-" + column_name}, inplace=True)
    merged_table = pd.merge(df, df2, on='movieID')
    return merged_table, list1


def jjpd2():
    df = pd.read_csv('E:/Users/Przemeczek/PycharmProjects/WTILAB/user_ratedmovies.dat', nrows=100000, delimiter="\t",
                     usecols=["userID", "movieID", "rating"])
    df1 = pd.read_csv('E:/Users/Przemeczek/PycharmProjects/WTILAB/movie_genres.dat',
                      delimiter="\t",
                      usecols=["movieID", "genre"]).groupby('movieID')['genre'].apply(list)
    result = pd.merge(df, df1, on='movieID', how='inner')
    print(result)


# <!-- lab 4 -->

def mean_genres(df3, list1, flag=False):
    ratings = df3["rating"].values.reshape(-1, 1)
    movie_genres_cols_values = df3[list1].values
    ratings_genres = ratings * movie_genres_cols_values
    avg_ratings = np.nanmean(ratings_genres, axis=0)
    avg_ratings[np.isnan(avg_ratings)] = 0
    ratings_genres[np.isnan(ratings_genres)] = 0
    if flag:
        avg_matrix = avg_ratings.reshape(1, -1)
        ratings_genres = ratings_genres - avg_matrix
    data_raitings = pd.DataFrame(ratings_genres, columns=list1)
    df2 = df3.drop(list1, axis=1)
    retdef = df2.join(data_raitings)
    return avg_ratings, retdef


def user_genres_mean(df3, list1, id):
    _, retdef = mean_genres(df3, list1, flag=False)
    usr_rating = retdef.loc[retdef['userID'] == id]
    usr_rating[usr_rating == 0] = np.NaN
    avg_ratings = np.nanmean(usr_rating[list1].values, axis=0)
    avg_ratings[np.isnan(avg_ratings)] = 0
    return avg_ratings


def user_profile(df3, list1, id):
    avg_ratings, _ = mean_genres(df3, list1, flag=False)
    usr_avg = user_genres_mean(df3, list1, id)
    usr_profile = usr_avg - avg_ratings
    return usr_profile


def load_data():
    r2.flushdb()
    df, lista = jjpd()
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
    print(df)
    lista = df.columns.values.tolist()
    lista.remove("movieID")
    lista.remove("rating")
    lista.remove("userID")
    for i in df.userID.unique():
        usr = user_profile(df, lista, i)
        for h in range(len(lista)):
            r.hset(i.astype(str), lista[h], usr[h])
    print("completed")

def update_user(id):
    df = get_data()
    lista = df.columns.values.tolist()
    lista.remove("movieID")
    lista.remove("rating")
    lista.remove("userID")
    usr = user_profile(df, lista, id)
    for h in range(len(lista)):
        r.hset(id.astype(str), lista[h], usr[h])
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



if __name__ == '__main__':
    load_data()
    load_users()
    df = {'movieID': 31617.0, 'rating': 3.5, 'genre-Action': 1.0, 'genre-Adventure': 1.0, 'genre-Animation': 0, 'genre-Children': 0, 'genre-Comedy': 0, 'genre-Crime': 0, 'genre-Documentary': 0, 'genre-Drama': 1.0, 'genre-Fantasy': 0, 'genre-Film-Noir': 0, 'genre-Horror': 0, 'genre-IMAX': 0, 'genre-Musical': 0, 'genre-Mystery': 0, 'genre-Romance': 1.0, 'genre-Sci-Fi': 0, 'genre-Short': 0, 'genre-Thriller': 0, 'genre-War': 1.0, 'genre-Western': 0, 'userID': 75.0}
    print(get_rand_user())
    # print(df)
    print(get_user(75))
    update_user(75)
    add_ocena(df)
    print(get_user(75))
    # print(get_data())
    # print(get_users())
