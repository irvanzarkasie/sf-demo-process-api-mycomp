import redis
import json
import urllib3
from datetime import datetime

# Initializing the redis instance
r = redis.Redis(
    host='127.0.0.1',
    port=36379,
    decode_responses=True # <-- this will ensure that binary data is decoded
)
COMM_CHANNEL = "mycomp-get-routes-req-channel"

# HTTP connection pool
http = urllib3.PoolManager()

# Backend identifier
BACKENDID = "B2U"

subchannel = r.pubsub()
subchannel.subscribe(COMM_CHANNEL)
for message in subchannel.listen():
    print(message)
    if isinstance(message.get("data", ""), str):
        req_payload = json.loads(message.get("data", ""))
        req_corr_id = req_payload.get("correlation_id", "")
        req_msg_ts = req_payload.get("msg_timestamp", "")
        req_departure_code = req_payload.get("departure_code", "")
        req_destination_code = req_payload.get("destination_code", "")

        if req_corr_id.startswith("B2U_"):
            base_req_url = "http://168.119.225.15:35020/sys/b2u/booking/routes"
            if req_departure_code != "" and req_destination_code != "":
                req_url = "{base_req_url}?departureCode={departure_code}&destinationCode={destination_code}".format(base_req_url=base_req_url, departure_code=req_departure_code, destination_code=req_destination_code)
            # end if
            elif req_departure_code != "" and req_destination_code == "":
                req_url = "{base_req_url}?departureCode={departure_code}".format(base_req_url=base_req_url, departure_code=req_departure_code)
            # end if
            elif req_departure_code == "" and req_destination_code != "":
                req_url = "{base_req_url}?destinationCode={destination_code}".format(base_req_url=base_req_url, destination_code=req_destination_code)
            # end if
            else:
                req_url = base_req_url
            # end else

            print("BUS2U", req_url)
            resp = http.request("GET", req_url)
            resp_payload = json.loads(resp.data.decode("utf-8"))
            resp_ts = int(datetime.now().timestamp())

            store_corr_id = "{req_correlation_id}".format(req_correlation_id=req_corr_id)
            store_payload = {
                "correlation_id": store_corr_id,
                "resp_payload": json.dumps(resp_payload),
                "req_ts": req_msg_ts,
                "resp_ts": resp_ts
            }
            r.set(store_corr_id, json.dumps(store_payload))
        #end if
    # end if

# end for