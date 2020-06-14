from flask import Flask, jsonify, abort, request
from wtiproj07_elasticsearch_simple_client import ElasticClient


app = Flask(__name__)
es = ElasticClient()
# ------ Simple operations ------
@app.route("/user/document/<id>", methods=["GET"])


def get_user(id):
    try:
        index = request.args.get('index', default='users')
        result = es.get_movies_liked_by_user(id, index)
        return jsonify(result)
    except:
        abort(404)

@app.route("/movie/document/<id>", methods=["GET"])
def get_movie(id):
    try:
        index = request.args.get('index', default='movies')
        result = es.get_users_that_like_movie(id, index)
        return jsonify(result)
    except:
        abort(404)

@app.route("/user/preselection/<id>", methods=["GET"])
def use_preselection(id):
    print(id)
    try:
        result = es.collaboratoryFilterUsers(int(id))
        print(result)
        result = {
        "moviesFound": result
        }
        return jsonify(result)
    except:
        abort(404)

@app.route("/movie/preselection/<id>", methods=["GET"])
def movies_preselection(id):
    try:
        result = es.colaboratoryFileterMovie(int(id))
        result = {
        "usersFound": result
        }
        return jsonify(result)
    except:

        abort(404)

# ------ Add/Update/Delete ------
@app.route("/user/document/<user_id>", methods=["PUT"])
def add_user_document(user_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        movies_liked_by_user = request.json
        es.addUser(user_id, movies_liked_by_user)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["PUT"])
def add_movie_document(movie_id):
    try:

        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        users_who_like_movie = request.json
        print('Movie id: {}'.format(movie_id))
        print('Users who like: {}'.format(users_who_like_movie))
        es.addMovie(movie_id, users_who_like_movie)
        return "Ok", 200
    except:
        abort(400)

@app.route("/user/document/<user_id>", methods=["POST"])
def update_user_document(user_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        movies_liked_by_user = request.json
        es.updateUser(user_id, movies_liked_by_user)
        return "Ok", 200
    except:
        abort(400)

@app.route("/movie/document/<movie_id>", methods=["POST"])
def update_movie_document(movie_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        users_who_like_movie = request.json
        es.updateMovie(movie_id, users_who_like_movie)
        return "Ok", 200
    except:
        abort(400)

@app.route("/user/document/<user_id>", methods=["DELETE"])
def delete_user_document(user_id):
    try:
        es.deleteUser(user_id)
        return "Ok", 200
    except:
        abort(400)

@app.route("/movie/document/<movie_id>", methods=["DELETE"])
def delete_movie_document(movie_id):
    try:
        es.deleteMovie(movie_id)
        return "Ok", 200
    except:
        abort(400)

@app.route("/user/bulk", methods=["POST"])
def bulk_update_users():
    """
    Body should look like this: [{"user_id": 123, "liked_movies": [1,2,3,4]}, ...]
    """
    print('User bulk')
    index = request.args.get('index', default='users')
    body = request.json
    print('body: {}'.format(body))
    print('index: {}'.format(index))
    es.bulk_user_update(body, index)
    return 'Ok', 200

@app.route("/movie/bulk", methods=["POST"])
def bulk_update_movies():
    """
    Body should look like this: [{"movie_id": 123, "users_who_liked_movie": [1,2,3,4]}, ...]
    """
    index = request.args.get('index', default='movies')
    body = request.json
    es.bulk_movie_update(body, index)
    return 'Ok', 200


if __name__ == '__main__':
    es.index_documents()
    app.run(debug=True)