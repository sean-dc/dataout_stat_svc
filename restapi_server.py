from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from restful_dataout_stat import dataout_stat
from singleton_meta import Singleton
from flask_cors import CORS, cross_origin

class restapi_server(metaclass=Singleton):
    def __init__(self):     
        self.flask = Flask("dataout_stat")
        CORS(self.flask)
        self.api = Api(self.flask)        
        return

    def add_resource(self, resource, uri):
        self.api.add_resource(resource, uri)
        return

    def run(self, debug, _host, _port):
        self.flask.run(debug=debug, host=_host, port=_port)

def start_rest_api_server(port=9201):
    rest_server = restapi_server()
    rest_server.add_resource(dataout_stat, '/dataout_stat/<id>')
    rest_server.run(debug=False, _host="0.0.0.0", _port=port)