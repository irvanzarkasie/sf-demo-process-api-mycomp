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
from time import sleep

app = Flask(__name__)
api = Api(app)

# CONSTANTS
api_host = socket.gethostname()
api_port = 36000
api_id = "mycomp_process_api"
WAIT_TIMEOUT = 3000
BACKOFF_MS = 50

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

# Hash map for departure/destination code
DEPDESTCODEMAP = {
  "MY-01": {
    "code": "PRTKLG-MY",
    "desc": "Port Klang Malaysia"
  },
  "MY-02": {
    "code": "BUKL-MY",
    "desc": "Bandar Utama KL Malaysia"
  },
  "SG-01": {
    "code": "HARBOUR-SG",
    "desc": "Harbourfront Centre Singapore"
  },
  "SG-02": {
    "code": "BEU-SG",
    "desc": "Buena Vista singapore"
  }
}

# Hash map for transport type code
TRANSTYPECODEMAP = {
   "9001": {
      "code": "ECOM-B1", 
      "desc": "BUS"
   },
   "9002": {
      "code": "ECOM-SH",
      "desc": "SHIP"
   },
   "9003": {
      "code": "ECOM-VN",
      "desc": "VAN"
   },
   "9004": {
      "code": "ECOM-MPV",
      "desc": "MPV"
   },
   "9005": {
      "code": "ECOM-TAXI",
      "desc": "Executive Taxi"
   }
}

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

      # Poll response from backend
      curr_ts = int(datetime.now().timestamp() * 1000)
      b2u_corr_id = "{correlation_id}_B2U".format(correlation_id=correlation_id)
      easycomego_corr_id = "{correlation_id}_EASYCOMEGO".format(correlation_id=correlation_id)
      b2u_resp = None
      easycomego_resp = None
      while int(datetime.now().timestamp() * 1000) - curr_ts <= WAIT_TIMEOUT and b2u_resp is None and easycomego_resp is None:
        print("Polling response from backend...")

        if b2u_resp is None:
          b2u_resp = r.get(b2u_corr_id)
        # end if
        if easycomego_resp is None:
          easycomego_resp = r.get(easycomego_corr_id)
        # end if

        print("BUS2U Response: {b2u_resp}".format(b2u_resp=b2u_resp))
        print("EASYCOMEEASYGO Response: {easycomego_resp}".format(easycomego_resp=easycomego_resp))

        # Eagerly exit poll if responses are already populated
        if b2u_resp is not None and easycomego_resp is not None:
          break
        # end if

        # Backoff before polling again
        sleep(BACKOFF_MS/1000)
      # end while

      b2u_resp_dict = json.loads(b2u_resp)
      b2u_resp_payload = json.loads(b2u_resp_dict.get("resp_payload", ""))

      easycomego_resp_dict = json.loads(easycomego_resp)
      easycomego_resp_payload = json.loads(easycomego_resp_dict.get("resp_payload", ""))

      # {
      #   "transportType": "",
      #   "routes": [
      #       {
      #           "departureCode": "",
      #           "departureDescription": "",
      #           "destinations": [
      #               {
      #                   "destinationCode": "",
      #                   "destinationDescription": ""
      #               }
      #           ]
      #       }
      #   ]
      # }
      
      resp_list = []

      # BUS2U TRANSFORMATION
      for route in b2u_resp_payload:
        mapped_dep_code = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("code", "")
        mapped_dep_desc = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("desc", "")
        mapped_dest_code = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("code", "")
        mapped_dest_desc = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("desc", "")
        mapped_trans_code = "ECOM-B1"
        mapped_trans_desc = "BUS"
        resp_list.append({
            "transportType": mapped_trans_code,
            "routes": [
                {
                    "departureCode": mapped_dep_code,
                    "departureDescription": mapped_dep_desc,
                    "destinations": [
                        {
                            "destinationCode": mapped_dest_code,
                            "destinationDescription": mapped_dest_desc
                        }
                    ]
                }
            ]
        })
      # end for

      # EASYCOMEASYGO TRANSFORMATION
      for route in easycomego_resp_payload:
        mapped_dep_code = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("code", "")
        mapped_dep_desc = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("desc", "")
        mapped_dest_code = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("code", "")
        mapped_dest_desc = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("desc", "")
        mapped_trans_code = TRANSTYPECODEMAP.get(route.get("transportCode", ""), "").get("code", "")
        mapped_trans_desc = TRANSTYPECODEMAP.get(route.get("transportCode", ""), "").get("desc", "")
        resp_list.append({
            "transportType": mapped_trans_code,
            "routes": [
                {
                    "departureCode": mapped_dep_code,
                    "departureDescription": mapped_dep_desc,
                    "destinations": [
                        {
                            "destinationCode": mapped_dest_code,
                            "destinationDescription": mapped_dest_desc
                        }
                    ]
                }
            ]
        })
      # end for


      return jsonify(resp_list)
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

      # Poll response from backend
      curr_ts = int(datetime.now().timestamp() * 1000)
      b2u_corr_id = "{correlation_id}_B2U".format(correlation_id=correlation_id)
      easycomego_corr_id = "{correlation_id}_EASYCOMEGO".format(correlation_id=correlation_id)
      b2u_resp = None
      easycomego_resp = None
      while int(datetime.now().timestamp() * 1000) - curr_ts <= WAIT_TIMEOUT and b2u_resp is None and easycomego_resp is None:
        print("Polling response from backend...")

        if b2u_resp is None:
          b2u_resp = r.get(b2u_corr_id)
        # end if
        if easycomego_resp is None:
          easycomego_resp = r.get(easycomego_corr_id)
        # end if

        print("BUS2U Response: {b2u_resp}".format(b2u_resp=b2u_resp))
        print("EASYCOMEEASYGO Response: {easycomego_resp}".format(easycomego_resp=easycomego_resp))

        # Eagerly exit poll if responses are already populated
        if b2u_resp is not None and easycomego_resp is not None:
          break
        # end if

        # Backoff before polling again
        sleep(BACKOFF_MS/1000)
      # end while

      b2u_resp_dict = json.loads(b2u_resp)
      b2u_resp_payload = json.loads(b2u_resp_dict.get("resp_payload", ""))

      easycomego_resp_dict = json.loads(easycomego_resp)
      easycomego_resp_payload = json.loads(easycomego_resp_dict.get("resp_payload", ""))

      # {
      #   "transportType": "",
      #   "routes": [
      #       {
      #           "departureCode": "",
      #           "departureDescription": "",
      #           "destinations": [
      #               {
      #                   "destinationCode": "",
      #                   "destinationDescription": ""
      #               }
      #           ]
      #       }
      #   ]
      # }
      
      resp_list = []

      # BUS2U TRANSFORMATION
      for route in b2u_resp_payload:
        mapped_dep_code = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("code", "")
        mapped_dep_desc = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("desc", "")
        mapped_dest_code = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("code", "")
        mapped_dest_desc = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("desc", "")
        mapped_trans_code = "ECOM-B1"
        mapped_trans_desc = "BUS"
        resp_list.append({
            "transportType": mapped_trans_code,
            "routes": [
                {
                    "departureCode": mapped_dep_code,
                    "departureDescription": mapped_dep_desc,
                    "destinations": [
                        {
                            "destinationCode": mapped_dest_code,
                            "destinationDescription": mapped_dest_desc
                        }
                    ]
                }
            ]
        })
      # end for

      # EASYCOMEASYGO TRANSFORMATION
      for route in easycomego_resp_payload:
        mapped_dep_code = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("code", "")
        mapped_dep_desc = DEPDESTCODEMAP.get(route.get("departureCode", ""), "").get("desc", "")
        mapped_dest_code = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("code", "")
        mapped_dest_desc = DEPDESTCODEMAP.get(route.get("destinationCode", ""), "").get("desc", "")
        mapped_trans_code = TRANSTYPECODEMAP.get(route.get("transportCode", ""), "").get("code", "")
        mapped_trans_desc = TRANSTYPECODEMAP.get(route.get("transportCode", ""), "").get("desc", "")
        resp_list.append({
            "transportType": mapped_trans_code,
            "routes": [
                {
                    "departureCode": mapped_dep_code,
                    "departureDescription": mapped_dep_desc,
                    "destinations": [
                        {
                            "destinationCode": mapped_dest_code,
                            "destinationDescription": mapped_dest_desc
                        }
                    ]
                }
            ]
        })
      # end for


      return jsonify(resp_list)
    # end def
# end class

api.add_resource(MyCompProcApi, '/proc/booking/<transport_type>/routes')
api.add_resource(MyCompProcApiDefault, '/proc/booking/routes')

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=api_port)
# end if