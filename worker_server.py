import zerorpc
import gevent
from config import Config
from jobs import *
import splitter
import os, errno


class Worker(object):
    no_of_reducers = 0
    completed_map_jobs = {}
    completed_reduce_jobs = set()
    shuffled_data = {}
    engine = None
    job_name = None
    job_type = None

    master_ip = ""

    status = Config.WORKER_STATUS_IDLE

    c = zerorpc.Client()

    def get_printable_status (self, status):
        if status == Config.WORKER_STATUS_REDUCE_FAILED:
            return "FAILED"
        elif status == Config.WORKER_STATUS_IDLE:
            return "Idle"
        elif status == Config.WORKER_STATUS_COMPLETE:
            return "Completed"
        elif status == Config.WORKER_STATUS_WORKING_MAP:
            return "MapTask"
        elif status == Config.WORKER_STATUS_WORKING_REDUCE:
            return "ReduceTask"
        elif status == Config.WORKER_STATUS_WORKING_SHUFFLE:
            return "ShuffleTask"
        else:
            return str(status)

    def __init__(self, master_ip):
        Worker.master_ip = master_ip
        self.my_ip = sys.argv[1]
        Worker.c.connect(Worker.master_ip)
        gevent.spawn(self.heart_beat)

    def heart_beat(self):
        while True:
            self.send_heart_beat(None)
            gevent.sleep(Config.HEART_BEAT_TIME_INTREVAL)

    def send_heart_beat(self, job_id):
        print("[hear_beat_send .... Job_ID - {0} : {1}] ...").format(job_id, self.get_printable_status(Worker.status))
        Worker.c.process_heart_beat(self.my_ip, Worker.status, job_id, Worker.job_type)
        # reset the status of the worker after completed or failed task to idle

        if Worker.status == Config.WORKER_STATUS_IDLE:
            Worker.job_type = None

        if Worker.status == Config.WORKER_STATUS_COMPLETE or Worker.status == Config.WORKER_STATUS_REDUCE_FAILED:
            Worker.shuffled_data = {}
            # if len(Worker.completed_reduce_jobs) == Worker.no_of_reducers and Worker.no_of_reducers > 0:
            #     self.reset_worker()
            Worker.status = Config.WORKER_STATUS_IDLE


    def start_map_job(self, job_name, input_file, start_index, chunk_size, no_of_reducers):
        if Worker.job_name != job_name:
            self.reset_worker()
        Worker.job_name = job_name
        Worker.no_of_reducers = no_of_reducers
        # if(job_name == Jobs.WORD_COUNT_JOB):
        gevent.spawn(self.do_map_job, job_name, input_file, start_index, chunk_size, no_of_reducers)
        return True

    def start_reduce_job(self, reducer_id, other_reducers):
        # if(job_name == Jobs.WORD_COUNT_JOB):
        gevent.spawn(self.do_reduce_job, reducer_id, other_reducers)
        return True

    def do_map_job(self, job_name, input_file, start_index, chunk_size, no_of_reducers):
        print "[............... Started Map Job_Id {0} ........... ]".format(start_index)

        Worker.job_type = Config.WORKER_STATUS_WORKING_MAP
        job_id = start_index
        Worker.status = Config.WORKER_STATUS_WORKING_MAP
        gevent.sleep(
            Config.MIN_TIME_TO_PREEMPT_TASK)  # this is done to reply to the master, so that the client doesn't timed out
        map_output = self.map_function(job_name, chunk_size, input_file, no_of_reducers, start_index)

        # Worker.status = Config.WORKER_STATUS_WORKING_REDUCE
        # self.do_reduce_job(engine, map_output)

        self.process_completed_map_job(job_id, map_output)

    def map_function(self, job_name, chunk_size, input_file, no_of_reducers, start_index):
        map_output = ""
        if job_name == Jobs.WORD_COUNT_JOB:
            values = splitter.read_chunk_by_word(input_file, start_index, chunk_size)
            engine = mapreduce.Engine(values, WordCountMap, WordCountReduce, mapreduce.Partition, no_of_reducers)
            Worker.engine = engine
            map_output = engine.map_phase()
        elif job_name == Jobs.SORTING_JOB:
            values = splitter.read_chunk_by_word(input_file, start_index, chunk_size)
            engine = mapreduce.Engine(values, SortingMap, SortingReduce, mapreduce.Partition, no_of_reducers)
            Worker.engine = engine
            map_output = engine.map_phase()
        elif job_name == Jobs.HAMMING_ENCODE_JOB:
            values = splitter.read_chunk(input_file, start_index, chunk_size)
            engine = hamming_mapreduce.Engine(values, HammingEncodingMap, HammingEncodingReduce, mapreduce.Partition,
                                              no_of_reducers, start_index)
            Worker.engine = engine
            map_output = engine.hamming_encode_map_phase()
        elif job_name == Jobs.HAMMING_DECODE_JOB:
            values = splitter.read_binary_chunk(input_file, start_index, chunk_size)
            engine = hamming_mapreduce.Engine(values, HammingDecodingMap, HammingDecodingReduce, mapreduce.Partition,
                                              no_of_reducers, start_index)
            Worker.engine = engine
            map_output = engine.hamming_decode_map_phase()
        elif job_name == Jobs.HAMMING_ERROR_JOB:
            values = splitter.read_binary_chunk(input_file, start_index, chunk_size)
            engine = hamming_mapreduce.Engine(values, HammingErrorMap, HammingErrorReduce, mapreduce.Partition,
                                              no_of_reducers, start_index)
            Worker.engine = engine
            map_output = engine.hamming_error_map_phase()
        elif job_name == Jobs.HAMMING_CHECK_JOB:
            values = splitter.read_binary_chunk(input_file, start_index, chunk_size)
            engine = hamming_mapreduce.Engine(values, HammingCheckMap, HammingCheckReduce, mapreduce.Partition,
                                              no_of_reducers, start_index)
            Worker.engine = engine
            map_output = engine.hamming_check_map_phase()
        elif job_name == Jobs.HAMMING_FIX_JOB:
            values = splitter.read_binary_chunk(input_file, start_index, chunk_size)
            engine = hamming_mapreduce.Engine(values, HammingFixMap, HammingFixReduce, mapreduce.Partition,
                                              no_of_reducers,
                                              start_index)
            Worker.engine = engine
            map_output = engine.hamming_fix_map_phase()
        else:
            print "Invalid Job Name ............."


        Worker.engine = engine

        # elif job_name == Jobs.HAMMING_ENCODE_JOB:

        return map_output

    def process_completed_map_job(self, job_id, map_output):
        Worker.status = Config.WORKER_STATUS_COMPLETE
        Worker.job_type = Config.WORKER_STATUS_WORKING_MAP
        Worker.completed_map_jobs[job_id] = map_output
        gevent.spawn(self.send_heart_beat, job_id)

    def process_completed_reduce_job(self, job_id):
        Worker.status = Config.WORKER_STATUS_COMPLETE
        Worker.job_type = Config.WORKER_STATUS_WORKING_REDUCE
        Worker.completed_reduce_jobs.add(job_id)
        gevent.spawn(self.send_heart_beat, job_id)

    def shuffle_info(self, reducer_id):
        map_data = {}
        for job_id in Worker.completed_map_jobs.keys():
            values = Worker.completed_map_jobs[job_id][reducer_id]
            map_data = self.shuffle_merge(map_data, values)
        return map_data

    def do_reduce_job(self, reducer_id, other_workers):
        try:
            print "[............... Started Reduce Job_Id {0} ........... ]".format(reducer_id)
            Worker.status = Config.WORKER_STATUS_WORKING_REDUCE
            Worker.job_type = Config.WORKER_STATUS_WORKING_REDUCE
            temp_data = self.shuffle_phase(reducer_id, other_workers)
            self.reduce_phase(reducer_id, temp_data)
            self.process_completed_reduce_job(reducer_id)
            return True
        except Exception as e:
            print "Error: " + str(e)
            Worker.status = Config.WORKER_STATUS_REDUCE_FAILED
            gevent.spawn(self.send_heart_beat, reducer_id)
            return False

    def get_remote_shuffle_data(self, reducer_id, other_worker_id):
        worker_connect = zerorpc.Client()
        worker_connect.connect(other_worker_id)
        shuffle_data = worker_connect.shuffle_info(reducer_id)
        worker_connect.close()
        return shuffle_data

    def shuffle_phase(self, reducer_id, other_workers):
        Worker.status = Config.WORKER_STATUS_WORKING_SHUFFLE
        temp_shuffle_data = {}
        Worker.shuffled_data = {}
        for worker_id in other_workers:
            temp_shuffle_data = self.shuffle_merge(temp_shuffle_data,
                                                      self.get_remote_shuffle_data(reducer_id, worker_id))
        # get local shuffle data
        local_data = self.shuffle_info(reducer_id)
        temp_shuffle_data = self.shuffle_merge(temp_shuffle_data, local_data)
        return temp_shuffle_data

    def shuffle_merge(self, data_1, data_2):
        for k in data_2.keys():
            if k in data_1:
                new_list = data_1[k] + data_2[k]
                data_1[k] = new_list
            else:
                data_1[k] = data_2[k]
            gevent.sleep(Config.MIN_TIME_TO_PREEMPT_TASK)
        return data_1

    def silentremove(self, filename):
        try:
            os.remove(filename)
        except OSError as e:  # this would be "except OSError, e:" before Python 2.6
            # if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            pass  # re-raise exception if a different error occured

    def reduce_phase(self, reducer_id, shuffle_data):
        # print shuffle_data.keys()
        result_list = Worker.engine.reduce_phase(shuffle_data)
        file_name = Config.OUTPUT_DIR + Worker.job_name + "_" + str(reducer_id)
        self.silentremove(file_name)
        f = open(file_name, 'w')
        if Worker.job_name == Jobs.HAMMING_ENCODE_JOB or Worker.job_name == Jobs.HAMMING_ERROR_JOB or Worker.job_name == Jobs.HAMMING_FIX_JOB or Worker.job_name == Jobs.HAMMING_DECODE_JOB:
            encode_string = ''
            for r in result_list:
                encode_string += r
            f.write(encode_string)
            f.close()
        else:
            for r in result_list:
                f.write(str(r) + '\n')  # python will convert \n to os.linesep
            f.close()  # you can omit in most cases as the destructor will call it

    def reset_worker(self):
        Worker.no_of_reducers = 0
        Worker.completed_map_jobs = {}
        Worker.completed_reduce_jobs = set()
        Worker.shuffled_data = {}
        Worker.engine = None
        Worker.job_name = None
        Worker.job_type = None
        print "............... Worker Reset Complete ....................."

s = zerorpc.Server(Worker(Config.MASTER_IP))
print(s.bind(sys.argv[1]))
s.run()
