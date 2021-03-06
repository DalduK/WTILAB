from flask import Flask, request, jsonify
import json
import wtiproj03_ETL
import random

api = Flask(__name__)
api.config['JSON_SORT_KEYS'] = False

df, list1 = wtiproj03_ETL.jjpd()
df = df.fillna(0)


@api.route('/rating', methods=['GET', 'POST', 'DELETE'])
def app():
    if request.method == 'POST':
        value = request.form
        global df
        df = df.append(value, ignore_index=True)
        return json.dumps(value)
    if request.method == 'GET':
        if df.empty:
            return jsonify('empty')
        else:
            value = df.sample(n=1)
            jsonfiles = json.loads(value.to_json(orient='records'))
            return jsonify(jsonfiles)
    if request.method == 'DELETE':
        df = df.iloc[0:0]
        if df.empty:
            return jsonify('')
        else:
            return jsonify(df.to_dict(orient='records'))


@api.route('/ratings', methods=['GET'])
def app1():
    if request.method == 'GET':
        if df.empty:
            return jsonify('empty')
        else:
            jsonfiles = json.loads(df.to_json(orient='records'))
            return jsonify(jsonfiles)


@api.route('/avg-genre-ratings/all-users', methods=['GET'])
def app2():
    if request.method == 'GET':
        if df.empty:
            return jsonify('empty')
        else:
            dict = {}
            for c in list1:
                dict[c] = random.uniform(0, 5)
            return jsonify(dict)


@api.route('/avg-genre-ratings/<user>', methods=['GET'])
def app3(user):
    if request.method == 'GET':
        if df.empty:
            return jsonify('empty')
        else:
            dict = {}
            for c in list1:
                dict[c] = random.uniform(0, 5)
            dict['userID'] = user
            return jsonify(dict)


if __name__ == '__main__':
    api.run()
