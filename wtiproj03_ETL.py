import json
import pandas as pd
import re

def jjpd():
    df = pd.read_csv('/home/przemysaw/PycharmProjects/wtiproj01/user_ratedmovies.dat', sep=" ", nrows=100,header=None,
                     delimiter="\t",
                     names=['userID', 'movieID', 'rating', 'date_day', 'date_month', 'date_year', 'date_hour',
                            'date_minute',
                            'date_second'])
    df1 = pd.read_csv('/home/przemysaw/PycharmProjects/wtiproj01/movie_genres.dat', sep=" ", nrows=1000 ,header=None,
                     delimiter="\t",
                     names=['movieID', 'genre'])
    result = pd.merge(df, df1, on='movieID', how='inner')
    dict ={}
    dict['userID'] = 0
    dict['movieID'] = 0
    dict['rating'] = 0.0
    dict['genre-Action'] = 0
    dict['genre-Adventure'] = 0
    dict['genre-Animation'] = 0
    dict['genre-Children'] = 0
    dict['genre-Comedy'] = 0
    dict['genre-Crime'] = 0
    dict['genre-Documentary'] = 0
    dict['genre-Drama'] = 0
    dict['genre-Fantasy'] = 0
    dict['genre-Film-Noir'] = 0
    dict['genre-Horror'] = 0
    dict['genre-IMAX'] = 0
    dict['genre-Musical'] = 0
    dict['genre-Mystery'] = 0
    dict['genre-Romance'] = 0
    dict['genre-Sci-Fi'] = 0
    dict['genre-Short'] = 0
    dict['genre-Thriller'] = 0
    dict['genre-War'] = 0
    dict['genre-Western'] = 0
    df3 = pd.DataFrame(columns=['userID', 'movieID', 'rating', 'genre-Action', 'genre-Adventure', 'genre-Animation', 'genre-Children', 'genre-Comedy',
                                'genre-Crime', 'genre-Documentary', 'genre-Drama', 'genre-Fantasy', 'genre-Film-Noir','genre-Horror', 'genre-IMAX', 'genre-Musical',
                                'genre-Mystery', 'genre-Romance', 'genre-Sci-Fi', 'genre-Short', 'genre-Thriller', 'genre-War', 'genre-Western'])
    for index, row in result.iterrows():
        if index == 0:
            earlier = row['userID']
            earlierm = row['movieID']
        if row['userID'] == earlier and row['movieID'] == earlierm:
            dict['userID'] = row['userID']
            dict['movieID'] = row['movieID']
            dict['rating'] = row['rating']
            genre = row['genre']
            dict['genre-'+genre] = 1
            earlier = row['userID']
            earlierm = row['movieID']
        else:
            df3 = df3.append(dict,ignore_index=True)
            dict['userID'] = row['userID']
            dict['movieID'] = row['movieID']
            dict['rating'] = row['rating']
            dict['genre-Action'] = 0
            dict['genre-Adventure'] = 0
            dict['genre-Animation'] = 0
            dict['genre-Children'] = 0
            dict['genre-Comedy'] = 0
            dict['genre-Crime'] = 0
            dict['genre-Documentary'] = 0
            dict['genre-Drama'] = 0
            dict['genre-Fantasy'] = 0
            dict['genre-Film-Noir'] = 0
            dict['genre-Horror'] = 0
            dict['genre-IMAX'] = 0
            dict['genre-Musical'] = 0
            dict['genre-Mystery'] = 0
            dict['genre-Romance'] = 0
            dict['genre-Sci-Fi'] = 0
            dict['genre-Short'] = 0
            dict['genre-Thriller'] = 0
            dict['genre-War'] = 0
            dict['genre-Western'] = 0
            genre = row['genre']
            dict['genre-' + genre] = 1
            earlier = row['userID']
            earlierm = row['movieID']
    return df3

# <!-- potrzebne do lab nr 4 -->
def oceny(nazwa,df3):
    i = 0.0
    sum = 0.0
    srednia = 0.0
    for index, row in df3.iterrows():
        if row[nazwa] == 1:
            i += 1
            sum += row['rating']
    if sum != 0 or i !=0:
        srednia = sum/i
    print('filmy z gatunku ' + nazwa + ' maja srednia ocen :' + str(srednia))
