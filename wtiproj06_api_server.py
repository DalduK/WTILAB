import json
import cherrypy
import wtiproj06_api_logic

dt = wtiproj06_api_logic.api()


@cherrypy.expose
@cherrypy.tools.json_out()
class rating(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        return dt.get()

    def POST(self, **data):
        return dt.post(data)



@cherrypy.expose
@cherrypy.tools.json_out()
class ratings(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        return dt.get_all()

    def DELETE(self):
        return dt.delet()

@cherrypy.expose
@cherrypy.tools.json_out()
class avg(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        return dt.avg_all()

@cherrypy.expose
@cherrypy.tools.json_out()
class avg2(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self,user):
        return dt.avg_usr(user)

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
