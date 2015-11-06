import zerorpc
import gevent
from config import Config
import time
from jobs import *
# import random
# import sys
# import time

# class Worker(object):
#     def __init__(self):
#         gevent.spawn(self.heart_beat)
#
#     def heart_beat(self):
#         while True:
#             print("[hear_beat_send]")
#             c = zerorpc.Client()
#             c.connect("tcp://127.0.0.1:9000")
#             c.process_heart_beat()
#             gevent.sleep(10)
#
#     def set_job(self, name):
#         print("[Working] %s", name)
#         for x in range(1, 10):
#             time.sleep(20) # delays for 5 seconds
#         return name
#
#
# s = zerorpc.Server(Worker())
# print(s.bind("tcp://0.0.0.0:9001"))
# s.run()
#



# class Client(object):
#     def __init__(self):
#         gevent.spawn(self.check_progress)
#
#     def check_progress(self):
#         while True:
#             print("[Check Progress]")
#             c = zerorpc.Client()
#             c.connect("tcp://127.0.0.1:9000")
#             progress = c.progress()
#             print (progress)
#             if progress == "100":
#                 break
#             gevent.sleep(5)
#
#
#     def start_job(self):
#         c = zerorpc.Client()
#         c.connect("tcp://127.0.0.1:9000")
#         c.start_job("tcp://127.0.0.1:9001")
#
# if __name__ == "__main__":
#
#     cilent = Client();
#     cilent.start_job();

# try:

job_name = sys.argv[1]
input_file = sys.argv[2]
split_size = int(sys.argv[3])
no_of_reducers = int(sys.argv[4])

c = zerorpc.Client()
c.connect(Config.MASTER_IP)
gevent.spawn(c.submit_job, job_name, input_file, split_size, no_of_reducers)

while True:
    time.sleep(2)
    progress = c.progress_update()
    print progress + "\n"
    if(progress == "Completed"):
        break


print "........... {0} Compeleted .........".format(job_name)

# except Exception, e:
#     print str(e)