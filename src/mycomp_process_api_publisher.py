import sys
import json
from flask import Flask, request, jsonify, make_response, Response
from flask_restful import Api, Resource
import pprint
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import uuid
import socket
from datetime import datetime, timedelta
import os
import urllib3
import redis
import uuid

app = Flask(__name__)
api = Api(app)

# CONSTANTS
api_host = socket.gethostname()
api_port = 36000
api_id = "mycomp_process_api"

# Work directory setup
script_dir = os.path.dirname(os.path.realpath(__file__))
home_dir = "/".join(script_dir.split("/")[:-1])
log_dir = "{home_dir}/logs".format(home_dir=home_dir)

# Initializing the redis instance
r = redis.Redis(
    host='127.0.0.1',
    port=36379,
    decode_responses=True # <-- this will ensure that binary data is decoded
)
COMM_CHANNEL = "mycomp-get-routes-req-channel"

class MyCompProcApi(Resource):
    def get(self, transport_type):
      # Parse arguments
      args = request.args
      departure_code = args.get("departureCode", None)
      destination_code = args.get("destinationCode", None)

      correlation_id = datetime.now().strftime("%Y%m%d%H%M%S") + str(uuid.uuid4()).replace("-","")
      msg_ts = int(datetime.now().timestamp())

      req_payload = {
        "correlation_id": correlation_id,
        "msg_timestamp": msg_ts,
        "transport_code": transport_type,
        "departure_code": departure_code,
        "destination_code": destination_code 
      }

      r.publish(COMM_CHANNEL, json.dumps(req_payload))

      return jsonify({})
    # end def
# end class

class MyCompProcApiDefault(Resource):
    def get(self):
      # Parse arguments
      args = request.args
      departure_code = args.get("departureCode", None)
      destination_code = args.get("destinationCode", None)

      correlation_id = datetime.now().strftime("%Y%m%d%H%M%S") + str(uuid.uuid4()).replace("-","")
      msg_ts = int(datetime.now().timestamp())

      req_payload = {
        "correlation_id": correlation_id,
        "msg_timestamp": msg_ts,
        "transport_type": None,
        "departure_code": departure_code,
        "destination_code": destination_code 
      }

      r.publish(COMM_CHANNEL, json.dumps(req_payload))

      return jsonify({})
    # end def
# end class

api.add_resource(MyCompProcApi, '/proc/booking/<transport_type>/routes')
api.add_resource(MyCompProcApiDefault, '/proc/booking/routes')

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=api_port)
# end if