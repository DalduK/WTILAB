import secrets

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

import wtiproj04_ETL_and_data_processing as ws
import wtiproj03_ETL as w
import json
import pandas as pd

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()


def create_keyspace(session, keyspace):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """ 
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """)


def create_data_table(session, keyspace, table):
    session.execute("""
            CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """(
            userID float,
            movieID	float,
            rating float,
            genre_Action float,
            genre_Adventure float,
            genre_Animation float,
            genre_Children float,
            genre_Comedy float,
            genre_Crime float,
            genre_Documentary float,
            genre_Drama float,
            genre_Fantasy float,
            genre_Film_Noir float,
            genre_Horror float,
            genre_IMAX float,
            genre_Musical float,
            genre_Mystery float,
            genre_Romance float,
            genre_Sci_Fi float,
            genre_Short float,
            genre_Thriller float,
            genre_War float,
            genre_Western float,
            PRIMARY KEY(userID,movieID)
            )
            """)


def create_usr_profile_table(session):
    session.execute("""
            CREATE TABLE IF NOT EXISTS usr_profiles.profiles(
            userID float,
            genre_Action float,
            genre_Adventure float,
            genre_Animation float,
            genre_Children float,
            genre_Comedy float,
            genre_Crime float,
            genre_Documentary float,
            genre_Drama float,
            genre_Fantasy float,
            genre_Film_Noir float,
            genre_Horror float,
            genre_IMAX float,
            genre_Musical float,
            genre_Mystery float,
            genre_Romance float,
            genre_Sci_Fi float,
            genre_Short float,
            genre_Thriller float,
            genre_War float,
            genre_Western float,
            PRIMARY KEY(userID)
            )
            """)


def push_data_table(session, keyspace, table, df):
    query = """INSERT INTO """ + keyspace + """.""" + table + """(userID,movieID, rating, genre_Action, genre_Adventure, genre_Animation, genre_Children,
                genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy, genre_Film_Noir, genre_Horror,
                genre_IMAX, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,
                genre_War, genre_Western) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    prepared = session.prepare(query)
    for index, rows in df.iterrows():
        print(rows)
        session.execute(prepared, (rows['userID'], rows['movieID'], rows['rating'], rows['genre-Action'], rows['genre-Adventure'],
                                   rows['genre-Animation'], rows['genre-Children'], rows['genre-Comedy'], rows['genre-Crime'],
                                   rows['genre-Documentary'], rows['genre-Drama'], rows['genre-Fantasy'], rows['genre-Film-Noir'],
                                   rows['genre-Horror'], rows['genre-IMAX'], rows['genre-Musical'], rows['genre-Mystery'],
                                   rows['genre-Romance'],rows['genre-Sci-Fi'], rows['genre-Short'], rows['genre-Thriller'],
                                   rows['genre-War'], rows['genre-Western']))

def push_data_table(session, keyspace, table, df):
    query = """INSERT INTO """ + keyspace + """.""" + table + """(userID, movieID, rating, genre_Action, genre_Adventure, genre_Animation, genre_Children,
                genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy, genre_Film_Noir, genre_Horror,
                genre_IMAX, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,
                genre_War, genre_Western) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    prepared = session.prepare(query)
    dc = df.to_dict()
    session.execute(prepared, (dc[0]['userID'],dc[0]['movieID'],dc[0]['rating'],dc[0]['genre-Action'],dc[0]['genre-Adventure'],
                            dc[0]['genre-Animation'],dc[0]['genre-Children'],dc[0]['genre-Comedy'],dc[0]['genre-Crime'],
                                dc[0]['genre-Documentary'],dc[0]['genre-Drama'],dc[0]['genre-Fantasy'],dc[0]['genre-Film-Noir'],
                                dc[0]['genre-Horror'],dc[0]['genre-IMAX'],dc[0]['genre-Musical'],dc[0]['genre-Mystery'],
                                dc[0]['genre-Romance'],dc[0]['genre-Sci-Fi'],dc[0]['genre-Short'],dc[0]['genre-Thriller'],
                                dc[0]['genre-War'],dc[0]['genre-Western']))


def push_usr_table(session):
    query = """INSERT INTO usr_profiles.profiles(userID, genre_Action, genre_Adventure, genre_Animation, genre_Children,
                genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy, genre_Film_Noir, genre_Horror,
                genre_IMAX, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,
                genre_War, genre_Western) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    prepared = session.prepare(query)
    df = get_data_table(session, "user_ratings", "user_avg_rating")
    df.fillna(value=pd.np.nan, inplace=True)
    lista = df.columns.values.tolist()
    print(lista)
    lista.remove("movieid")
    lista.remove("rating")
    lista.remove("userid")
    for i in df.userid.unique():
        usr = ws.user_profile2(df, lista, i)
        session.execute(prepared, (i, usr[0], usr[1], usr[2], usr[3], usr[4], usr[5], usr[6], usr[7], usr[8],
                                   usr[9], usr[10], usr[11], usr[12], usr[13], usr[14], usr[15], usr[16], usr[17],
                                   usr[18], usr[19]))


def get_data_table(session, keyspace, table):
    rows = session.execute("SELECT * FROM " + keyspace + "." + table + ";")
    df = pd.DataFrame(list(rows))
    return df

def get_usr_table(session, usr):
    session.row_factory = dict_factory
    rows = session.execute("SELECT * FROM usr_profiles.profiles where userid="+str(usr)+";")
    return list(rows)

def get_random_user(session):
    session.row_factory = dict_factory
    rows = session.execute("SELECT * FROM user_ratings.user_avg_rating ;")
    ret = secrets.choice(list(rows))
    return ret

def delete_table(session, keyspace, table):
    session.execute("DROP TABLE " + keyspace + "." + table + ";")

def clear_table(session, keyspace, table):
    session.execute("TRUNCATE " + keyspace + "." + table + ";")


if __name__ == '__main__':
    keyspace = "user_ratings"
    table = "user_avg_rating"
    keyspace2 = "usr_profiles"
    table2 = "profiles"
    # df, _ = w.jjpd()
    # names = df.userID.unique()
    # create_keyspace(session, keyspace)
    # create_data_table(session, keyspace, table)
    # push_data_table(session,keyspace,table,df)
    # print(get_data_table(session, keyspace, table))
    # create_keyspace(session,keyspace2)
    # create_usr_profile_table(session)
    # push_usr_table(session)
    # print(get_data_table(session, keyspace2, table2))
    print(get_usr_table(session,75.0))
