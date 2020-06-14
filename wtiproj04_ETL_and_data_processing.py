import pandas as pd
import numpy as np

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


def user_mean(df3, list1, id):
    _, retdef = mean_genres(df3, list1, flag=False)
    usr_rating = retdef.loc[retdef['userID'] == id]
    usr_rating[usr_rating == 0] = np.NaN
    avg_ratings = np.nanmean(usr_rating[list1].values, axis=0)
    avg_ratings[np.isnan(avg_ratings)] = 0
    return avg_ratings

#do zadania 6
def user_mean2(df3, list1, id):
    _, retdef = mean_genres(df3, list1, flag=False)
    usr_rating = retdef.loc[retdef['userid'] == id]
    usr_rating[usr_rating == 0] = np.NaN
    avg_ratings = np.nanmean(usr_rating[list1].values, axis=0)
    avg_ratings[np.isnan(avg_ratings)] = 0
    return avg_ratings

def user_profile(df3, list1, id):
    avg_ratings, _ = mean_genres(df3, list1, flag=False)
    usr_avg = user_mean(df3, list1, id)
    usr_profile = usr_avg - avg_ratings
    return usr_profile

def user_profile2(df3, list1, id):
    avg_ratings, _ = mean_genres(df3, list1, flag=False)
    usr_avg = user_mean2(df3, list1, id)
    usr_profile = usr_avg - avg_ratings
    return usr_profile