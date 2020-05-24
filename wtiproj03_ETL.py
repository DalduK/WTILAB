import pandas as pd
import numpy as np
# '/home/przemysaw/PycharmProjects/wtiproj01/user_ratedmovies.dat'
# '/home/przemysaw/PycharmProjects/wtiproj01/movie_genres.dat'

df = pd.read_csv('E:/Users/Przemeczek/PycharmProjects/WTILAB/user_ratedmovies.dat',nrows=50000, delimiter="\t",
                     usecols=["userID", "movieID", "rating"])
df1 = pd.read_csv('E:/Users/Przemeczek/PycharmProjects/WTILAB/movie_genres.dat',
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
    df = pd.read_csv('E:/Users/Przemeczek/PycharmProjects/WTILAB/user_ratedmovies.dat',  nrows=100000,delimiter="\t",
                  usecols=["userID", "movieID", "rating"])
    df1 = pd.read_csv('E:/Users/Przemeczek/PycharmProjects/WTILAB/movie_genres.dat',
                     delimiter="\t",
                     usecols=["movieID", "genre"]).groupby('movieID')['genre'].apply(list)
    result = pd.merge(df, df1, on='movieID', how='inner')
    print(result)

# <!-- lab 4 -->

def mean_genres(df3,list1,flag=False):
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
    retdef = df.join(data_raitings)
    return avg_ratings, retdef


def user_genres_mean(df3,list1, id):
    _,retdef = mean_genres(df3,list1,flag=False)
    print(retdef)
    usr_rating = retdef.loc[retdef['userID'] == id]
    usr_rating[usr_rating == 0] = np.NaN
    avg_ratings = np.nanmean(usr_rating[list1].values, axis=0)
    avg_ratings[np.isnan(avg_ratings)] = 0
    return avg_ratings

def user_profile(df3,list1, id):
    avg_ratings,_ = mean_genres(df3, list1, flag=False)
    usr_avg = user_genres_mean(ret, ret2, id)
    usr_profile = usr_avg - avg_ratings
    return usr_profile


if __name__ == '__main__':
    ret, ret2 = jjpd()
    print(user_profile(ret, ret2, 4827))