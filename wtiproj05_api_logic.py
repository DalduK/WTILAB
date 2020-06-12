import json

import wtiproj05_API
from flask import Flask, request
api = Flask(__name__)
api.config['JSON_SORT_KEYS'] = False

data = wtiproj05_API.api()

@api.route('/rating',methods=['GET','POST'])
def app():
    if request.method == 'POST':
        value = request.form
        return data.post(value)

    if request.method == 'GET':
        return data.get()


@api.route('/ratings',methods=['GET','DELETE'])
def app1():
    if request.method == 'GET':
        return data.get_all()
    if request.method == 'DELETE':
        return data.delet()

@api.route('/avg-genre-ratings/all-users',methods=['GET'])
def app2():
    if request.method == 'GET':
        return data.avg_all()

@api.route('/avg-genre-ratings/<user>',methods=['GET'])
def app3(user):
    if request.method == 'GET':
        return data.avg_usr(user)

if __name__ == '__main__':
    api.run()
