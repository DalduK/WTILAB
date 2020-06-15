import pandas as pd
import numpy as np
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

    def get_preselection_for_user(self,user_id,index='users'):
        user_id=int(user_id)
        movies_rated_by_users = self.es.search(index=index,body={
            "query":{
                "term":{
                    "_id":user_id
                }
            }
        })["hits"]["hits"][0]["_source"]["ratings"]
        users_that_rated_at_least_one_movie_from_the_given_set_of_movies = self.es.search(
            index=index,body={
                'query':{
                    'terms':{
                        'ratings':movies_rated_by_users
                    }
                }
            }
        )["hits"]["hits"]
        unique_movies=set()
        for ratings in users_that_rated_at_least_one_movie_from_the_given_set_of_movies:
            if ratings["_id"]!=user_id:
                ratings=ratings['_source']['ratings']
                for rating in ratings:
                    if rating not in movies_rated_by_users:
                        unique_movies.add(rating)
        return list(unique_movies)

    def get_preselection_for_movie(self,movie_id,index='users'):
        movie_id=int(movie_id)
        movies_rated_by_users = self.get_movies_liked_by_user(movie_id)['ratings']
        users_that_rated_movies = self.es.search(index=index, body={
            "query": {
                "term": {
                    "_id": movie_id
                }
            }
        })["hits"]["hits"][0]["_source"]["whoRated"]
        movies_rated_by_at_least_one_of_given_users = self.es.search(
            index=index,body={
                'query':{
                    'terms':{
                        'whoRated':users_that_rated_movies
                    }
                }
            }
        )["hits"]["hits"]
        unique_users=set()
        for ratings in movies_rated_by_at_least_one_of_given_users:
            if ratings["_id"]!=movie_id:
                ratings=ratings['_source']['whoRated']
                for rating in ratings:
                    if rating not in movies_rated_by_users:
                        unique_users.add(rating)
        return list(unique_users)

    def add_user_document(self,user_id,liked_movies,user_index='users',movie_index='movies'):
        user_id=int(user_id)
        liked_movies=list(set(liked_movies))
        movies_to_update = [self.es.get(index=movie_index,id=movie_id,doc_type='movie')for movie_id in liked_movies]
        if len(movies_to_update)!=len(liked_movies):
            raise Exception("Error! User can't like unknown movie")
        for movie_document in movies_to_update:
            users_who_like_movie = movie_document["_source"]["whoRated"]
            users_who_like_movie.append(user_id)
            users_who_like_movie = list(set(users_who_like_movie))
            self.es.update(index = movie_index, id=movie_document["_id"],doc_type="movie",body={"doc":{"whoRated":users_who_like_movie}})

        self.es.create(index=user_index,id=user_id,body={
            "ratings":liked_movies
        },doc_type="user")

    def add_movie_document(self,movie_id,user_who_liked_movie,user_index='users',movie_index='movies'):
        movie_id=int(movie_id)
        user_who_liked_movie=list(set(user_who_liked_movie))
        users_to_update = [self.es.get(index=user_index,id=user_id,doc_type='user')for user_id in user_who_liked_movie]
        if len(users_to_update)!=len(user_who_liked_movie):
            raise Exception("Error! Movie can't be liked by unknown users")
        for user_document in users_to_update:
            movies_liked_by_user= user_document["_source"]["ratings"]
            movies_liked_by_user.append(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index, id=user_document["_id"],doc_type="user",body={"doc":{"ratings":movies_liked_by_user}})

        self.es.create(index=movie_index,id=movie_id,body={
            "whoRated":user_who_liked_movie
        },doc_type="movie")

    def update_user_document(self, user_id, liked_movies, user_index='users', movie_index='movies'):
        user_id = int(user_id)
        liked_movies = list(set(liked_movies))
        user_document_to_update = self.es.get(index=user_index,id=user_id,doc_type='user')
        old_set_of_liked_movies = user_document_to_update['_source']['ratings']
        movies_to_add_user = np.setdiff1d(liked_movies,old_set_of_liked_movies)
        movies_to_remove_user = np.setdiff1d(old_set_of_liked_movies,liked_movies)

        for movie_to_remove_user in movies_to_remove_user:
            movie_document = self.es.get(index=movie_index,id=movie_to_remove_user,doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.remove(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,id=movie_to_remove_user,doc_type='movie',body={"doc":{"whoRated":users_who_liked_movie}})

        for movie_to_add_user in movies_to_add_user:
            movie_document = self.es.get(index=movie_index, id=movie_to_add_user, doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.append(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index, id=movie_to_add_user, doc_type='movie',
                           body={"doc": {"whoRated": users_who_liked_movie}})

        self.es.update(index=user_index,id=user_id,body={"doc":{"ratings":liked_movies}},doc_type="user")

    def update_movie_document(self, movie_id, users_who_liked_movie, user_index='users', movie_index='movies'):
        movie_id = int(movie_id)
        users_who_liked_movie = list(set(users_who_liked_movie))
        movie_document_to_update = self.es.get(index=movie_index,id=movie_id,doc_type='movie')
        old_set_of_users_who_liked_movie = movie_document_to_update['_source']['whoRated']

        users_to_add_movie = np.setdiff1d(users_who_liked_movie,old_set_of_users_who_liked_movie)
        users_to_remove_movie = np.setdiff1d(old_set_of_users_who_liked_movie,users_who_liked_movie)

        for user_to_remove_movie in users_to_remove_movie:
            user_document = self.es.get(index=user_index,id=user_to_remove_movie,doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.remove(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,id=user_to_remove_movie,doc_type='user',
                           body={"doc":{"ratings": movies_liked_by_user}})

        for user_to_add_movie in users_to_add_movie:
            user_document = self.es.get(index=user_index, id=user_to_add_movie, doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.append(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index, id=user_to_add_movie, doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        self.es.update(index=movie_index,id=movie_id,body={"doc":{"whoRated":users_who_liked_movie}},doc_type="movie")

    def delete_user_document(self,user_id,user_index="users",movie_index="movies"):
        user_id = int(user_id)
        user_document_to_delete = self.es.get(index=user_index, id=user_id, doc_type='user')["_source"]["ratings"]

        for movie_id_to_update in user_document_to_delete:
            movie_document = self.es.get(index=movie_index,id=movie_id_to_update,doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.remove(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,id=movie_id_to_update,doc_type='movie',
                           body={"doc":{"whoRated":users_who_liked_movie}})

        self.es.delete(index=user_index,id=user_id,doc_type="user")

    def delete_movie_document(self,movie_id,user_index="users",movie_index="movies"):
        movie_id = int(movie_id)
        movie_document_to_delete = self.es.get(index=movie_index, id=movie_id, doc_type='movie')["_source"]["whoRated"]

        for user_id_to_update in movie_document_to_delete:
            user_document = self.es.get(index=user_index,id=user_id_to_update,doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.remove(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,id=user_id_to_update,doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        self.es.delete(index=movie_index,id=movie_id,doc_type="movie")

    def bulk_user_update(self,data,user_index='users',movie_index='movies'):
        for item in data:
            self.update_user_document(item['user_id'],item['liked_movies'],
                                      user_index=user_index,movie_index=movie_index)

    def bulk_movie_update(self,data,user_index='users',movie_index='movies'):
        for item in data:
            self.update_user_document(item['movie_id'],item['users_who_liked_movie'],
                                      user_index=user_index,movie_index=movie_index)


if __name__ == '__main__':
    ec = ElasticClient()
    # ec.index_documents()
    # ------ Simple operations ------
    print()
    user_document = ec.get_movies_liked_by_user(75)
    movie_id = np.random.choice(user_document['ratings'])
    movie_document = ec.get_users_that_like_movie(movie_id)
    random_user_id = np.random.choice(movie_document['whoRated'])
    random_user_document = ec.get_movies_liked_by_user(random_user_id)

    print('User 75 likes following movies:')
    print(user_document)

    print('Movie {} is liked by following users:'.format(movie_id))
    print(movie_document)

    print('Is user 75 among users in movie {} document?'.format(movie_id))
    print(movie_document['whoRated'].index(75) != -1)

    import random

    some_test_movie_ID = 1

    print("Some test movie ID: ", some_test_movie_ID)
    list_of_users_who_liked_movie_of_given_ID = ec.get_users_that_like_movie(some_test_movie_ID)["whoRated"]

    print("List of users who liked the test movie: ",
          *list_of_users_who_liked_movie_of_given_ID)

    index_of_random_user_who_liked_movie_of_given_ID = random.randint(0,
        len(list_of_users_who_liked_movie_of_given_ID))

    print("Index of random user who liked the test movie: ",
          index_of_random_user_who_liked_movie_of_given_ID)

    some_test_user_ID = list_of_users_who_liked_movie_of_given_ID[index_of_random_user_who_liked_movie_of_given_ID]

    print("ID of random user who liked the test movie: ", some_test_user_ID)
    movies_liked_by_user_of_given_id = ec.get_movies_liked_by_user(some_test_user_ID)["ratings"]

    print("IDs of movies liked by the random user who liked the test movie: ",
          *movies_liked_by_user_of_given_id)

    if some_test_movie_ID in movies_liked_by_user_of_given_id:
        print(
            "As expected, the test movie ID is among the IDs of movies " +
            "liked by the random user who liked the test movie ;-)")

    # preselection

    print()
    user_preselection = ec.get_preselection_for_user(75)
    print('Presel for user 75')
    print(user_preselection)

    print('Random user {} document who also liked movie {}'.format(random_user_id,movie_id))
    print(random_user_document['ratings'])

    print('Are moves rated by random user {} on recomended movies for user 75?'.format(random_user_id))
    intersect = np.intersect1d(user_preselection,random_user_document['ratings'])
    print(len(intersect)!=0)

    # add/update usr

    print()
    new_user_id = 50000
    print('Adding user with ID {} that liked movie 3 and 32'.format(new_user_id))
    ec.add_user_document(new_user_id,[3,32])
    update_movie_document = ec.get_users_that_like_movie(3)["whoRated"]
    print("After inserting new user with ID {}, movie 3 was updated: {}"
          .format(new_user_id,new_user_id in update_movie_document))

    print("Updating user with ID {}, now he likes 3 and 420".format(new_user_id))
    ec.update_user_document(new_user_id,[3,420])
    update_movie_document = ec.get_users_that_like_movie(420)["whoRated"]
    print("After update, user {} likes movie 420: {}".format(new_user_id,new_user_id in update_movie_document))
    update_movie_document = ec.get_users_that_like_movie(32)["whoRated"]
    print("After update, user {} dibt like movie 32: {}".format(new_user_id, new_user_id not in update_movie_document))

    print("Deleting user with ID {}".format(new_user_id))
    ec.delete_user_document(new_user_id)
    update_movie_document = ec.get_users_that_like_movie(420)["whoRated"]
    print("After update, user {} likes movie 420: {}".format(new_user_id, new_user_id not in update_movie_document))
    update_movie_document = ec.get_users_that_like_movie(3)["whoRated"]
    print("After update, user {} likes movie 3: {}".format(new_user_id, new_user_id not in update_movie_document))

    print()
    new_movie_id = 70000
    print('Adding movie with ID {} that is liked by 75 and 78'.format(new_movie_id))
    ec.add_movie_document(new_movie_id, [75, 78])
    update_user_document = ec.get_movies_liked_by_user(75)["ratings"]
    print("After inserting new user with ID {}, movie 3 was updated: {}"
          .format(new_movie_id, new_movie_id in update_user_document))

    print("Updating movie with ID {}, now it is liked 75 and 127".format(new_movie_id))
    ec.update_movie_document(new_movie_id, [75, 127])
    update_user_document = ec.get_movies_liked_by_user(127)["ratings"]
    print("After update, movie {} is liked by user 127: {}".format(new_movie_id, new_movie_id in update_user_document))
    update_user_document = ec.get_movies_liked_by_user(78)["ratings"]
    print("After update, movie {} is liked by user 127: {}".format(new_movie_id, new_movie_id in update_user_document))

    print("Deleting movie with ID {}".format(new_movie_id))
    ec.delete_movie_document(new_movie_id)
    updated_user_document = ec.get_movies_liked_by_user(75)["ratings"]
    print("After deletion of movie {}, document for user 75 is updated: {}"
          .format(new_movie_id,new_movie_id not in updated_user_document))
    updated_user_document = ec.get_movies_liked_by_user(127)["ratings"]
    print("After deletion of movie {}, document for user 127 is updated: {}"
          .format(new_movie_id, new_movie_id not in updated_user_document))