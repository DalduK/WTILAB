import pandas as pd
from elasticsearch import Elasticsearch, helpers

class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    # ------ Simple operations ------
    def index_documents(self):
        df = pd.read_csv('/home/dldk/PycharmProjects/WTILAB/user_ratedmovies.dat', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False).mean().loc[:, ['userID', 'rating']].rename(
            columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']
        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']].rename(
            columns={'ratingNormal': 'rating'}).pivot_table(index='userID', columns='movieID', values='rating').fillna(
            0)

        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0]
                    .sort_values(ascending=False)
                    .index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")

        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] >0]
                    .sort_values(ascending=False)
                    .index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def collab_user_search(self,userID):
        movieId = self.get_movies_liked_by_user(userID)['ratings']
        val = self.es.search(index='users', body={
            "query": {
                "bool": {
                    "must_not": {
                        "term": {
                            "_id": str(userID)
                        }
                    },
                    "filter": {
                        "terms": {"ratings": movieId}
                    }
                }
            }
        }, filter_path=['hits.hits._source'])["hits"]["hits"]
        movie_id = set()
        for item in val:
            for i in item['_source']['ratings']:
                movie_id.add(i)

        return list(movie_id)

    def collab_movie_search(self,movieID):
        userId = self.get_users_that_like_movie(movieID)["whoRated"]
        val = self.es.search(index='movies', body={
            "query": {
                "bool": {
                    "must_not": {
                        "term": {
                            "_id": str(movieID)
                        }
                    },
                    "filter": {
                        "terms": {"whoRated": userId}
                    }
                }
            }
        }, filter_path=['hits.hits._source'])['hits']['hits']
        user_id = set()
        for item in val:
            for i in item['_source']['whoRated']:
                user_id.add(i)
        return list(user_id)