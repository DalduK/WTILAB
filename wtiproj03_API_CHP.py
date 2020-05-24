import json
import random
import re

import cherrypy

import wtiproj03_ETL

df, list1 = wtiproj03_ETL.jjpd()
df = df.fillna(0)

@cherrypy.expose
@cherrypy.tools.json_out()
class rating(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        value = df.sample(n=1)
        jsonfiles = json.loads(value.to_json(orient='records'))
        return jsonfiles

    def POST(self, **data):
        global df
        print(data)
        df = df.append(data, ignore_index=True)
        return df.to_dict(orient='records')

    def DELETE(self):
        global df
        df = df.iloc[0:0]
        return json.dumps('')

@cherrypy.expose
@cherrypy.tools.json_out()
class ratings(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        jsonfiles = json.loads(df.to_json(orient='records'))
        return jsonfiles

@cherrypy.expose
@cherrypy.tools.json_out()
class avg(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        dict = {}
        for c in list1:
            dict[c] = random.uniform(0, 5)
        return dict

@cherrypy.expose
@cherrypy.tools.json_out()
class avg2(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self,user):
        dict = {}
        for c in list1:
            dict[c] = random.uniform(0, 5)
        dict['userID'] = user
        return dict

if __name__ == '__main__':
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        }
    }
    cherrypy.tree.mount(rating(),'/rating',conf)
    cherrypy.tree.mount(ratings(),'/ratings',conf)
    cherrypy.tree.mount(avg(), '/avg-genre-ratings/all-users', conf)
    cherrypy.tree.mount(avg2(), '/avg-genre-ratings/', conf)
    cherrypy.engine.start()
