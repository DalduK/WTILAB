import zmq
import time
import json

port = str(5555)
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("tcp://localhost:%s" % port)
job_ID = str(hash(time.time()))
msg_content_dict = {}
msg_content_dict["job_ID"] = job_ID
msg_content = str(json.dumps(msg_content_dict))
msg_string = msg_content.encode('ascii')
print("A new job request is about to be sent to the job executor.")
socket.send(msg_string)
print("Job requester is waiting for confirmation of the new job completion.")
msg = socket.recv()
msg_dict = json.loads(msg.decode('ascii'))
msg_dict_value = msg_dict[list(msg_dict)[0]]
if msg_dict_value == job_ID:
    print("Job requester has received the confirmation of the new job completion.")
else:
    print("Job requester has received some unexpected message.")