import pandas as pd
from elasticsearch import Elasticsearch, helpers
import wtiproj07_elasticsearch_simple_client as w

ec = w.ElasticClient()


def collab_user_search(userID):
    movieId = ec.get_movies_liked_by_user(userID)['ratings']
    val = ec.es.search(index='users', body={
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


def collab_movie_search(movieID):
    userId = ec.get_users_that_like_movie(movieID)["whoRated"]
    val = ec.es.search(index='movies', body={
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


if __name__ == '__main__':
    print(collab_user_search(75))
    print(collab_movie_search(1))
