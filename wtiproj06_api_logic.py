from cassandra.cluster import Cluster
import wtiproj03_ETL
import pandas as pd
import numpy as np
import redis

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()


def create_keyspace(session, keyspace):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS """+keyspace+""" 
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """)

def create_data_table(session,keyspace,table):
    session.execute("""
            CREATE TABLE IF NOT EXISTS """+ keyspace+"""."""+table+"""(
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

def push_data_table(session, keyspace,userID, df):
    query = """INSERT INTO """+ keyspace+"""."""+userID+"""(userID,movieID, rating, genre_Action, genre_Adventure, genre_Animation, genre_Children,
                genre_Comedy, genre_Crime, genre_Documentary, genre_Drama, genre_Fantasy, genre_Film_Noir, genre_Horror,
                genre_IMAX, genre_Musical, genre_Mystery, genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,
                genre_War, genre_Western) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    prepared = session.prepare(query)
    for index, rows in df.iterrows():
        session.execute(prepared, (rows[0],rows[1],rows[2],rows[3],rows[4],rows[5],rows[6],rows[7],rows[8],
                                   rows[9],rows[10],rows[11],rows[12],rows[13],rows[14],rows[15],rows[16],rows[17],
                                   rows[18],rows[19],rows[20]))

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
        usr = wtiproj03_ETL.user_profile(df, lista, i)
        session.execute(prepared, (i, usr[0],usr[1],usr[2],usr[3],usr[4],usr[5],usr[6],usr[7],usr[8],
                                    usr[9],usr[10],usr[11],usr[12],usr[13],usr[14],usr[15],usr[16],usr[17],
                                   usr[18],usr[19]))

def get_data_table(session, keyspace, table):
    rows = session.execute("SELECT * FROM "+keyspace+"."+table+";")
    df = pd.DataFrame(list(rows))
    return df

def delete_table(session, keyspace, table):
    session.execute("DROP TABLE "+keyspace+"."+table+";")


if __name__ == '__main__':
    keyspace = "user_ratings"
    table = "user_avg_rating"
    keyspace2 = "usr_profiles"
    table2 = "profiles"
    # delete_table(session, keyspace, table)
    # create_data_table(session)
    # df,_ = wtiproj03_ETL.jjpd()
    # names = df.userID.unique()
    # create_data_table(session, keyspace, table)
    # push_data_table(session,keyspace,table,df)
    # get_data_table(session, keyspace, table)
    # create_keyspace(session,"usr_profiles")
    # create_usr_profile_table(session)
    # push_usr_table(session)
    print(get_data_table(session,keyspace2,table2))
    # for i in names:
    #     nm = str(i)
    #     nm = 'n' + nm
    #
    # print(type(df))
    # push_data_table(session,df)
    # for i in names:
    #     nm = str(i)
    #     nm = 'n' + nm
    #     delete_table(session,keyspace,nm)