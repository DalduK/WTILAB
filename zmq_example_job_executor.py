import zmq
import time
import json

port = str(5555)
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.bind("tcp://*:%s" % port)

while True:
    print("Job executor is waiting for a new job request.")
    msg = socket.recv()
    print("Job executor has received a new job request.")
    msg_dict = json.loads(msg.decode('ascii'))
    print(msg_dict)
    msg_dict_value = msg_dict["job_ID"]
    print(msg_dict_value)
    print("Job execution is starting.")
    time.sleep(3)
    print("Job is completed.")
    print("Job executor is about to report the completion of the job back to the requester.")
    msg_content_dict = {}
    msg_content_dict["some_other_key"] = msg_dict_value
    msg_content = str(json.dumps(msg_content_dict))
    msg_string = msg_content.encode('ascii')
    socket.send(msg_string)