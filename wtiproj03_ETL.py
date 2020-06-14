import pandas as pd
import numpy as np
import redis

r = redis.Redis(host='localhost', port=32768, db=0)
r2 = redis.Redis(host='localhost', port=32768, db=1)

df = pd.read_csv('/home/dldk/PycharmProjects/WTILAB/user_ratedmovies.dat', nrows=20000, delimiter="\t",
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


if __name__ == '__main__':
    df = {'movieID': 31617.0, 'rating': 3.5, 'genre-Action': 1.0, 'genre-Adventure': 1.0, 'genre-Animation': 0,
          'genre-Children': 0,
          'genre-Comedy': 0, 'genre-Crime': 0, 'genre-Documentary': 0, 'genre-Drama': 1.0, 'genre-Fantasy': 0,
          'genre-Film-Noir': 0,
          'genre-Horror': 0, 'genre-IMAX': 0, 'genre-Musical': 0, 'genre-Mystery': 0, 'genre-Romance': 1.0,
          'genre-Sci-Fi': 0,
          'genre-Short': 0, 'genre-Thriller': 0, 'genre-War': 1.0, 'genre-Western': 0, 'userID': 75.0}
