import redis

# Initializing the redis instance
r = redis.Redis(
    host='127.0.0.1',
    port=36379,
    decode_responses=True # <-- this will ensure that binary data is decoded
)
COMM_CHANNEL = "mycomp-get-routes-req-channel"

subchannel = r.pubsub()
subchannel.subscribe(COMM_CHANNEL)
for message in subchannel.listen():
    print(message)
# end for