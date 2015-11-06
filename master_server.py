import zerorpc
from config import Config
import splitter
import gevent
import time


class Master(object):

    job_name = None
    input_file = None
    chunk_size = None

    no_of_reducers = 0
    progress = ""
    all_workers= set()
    workers_info = {} # "id" : None, "current_job" : None, "compeleted_jobs": set(), "last_received_hb": xxx

    split_map_jobs = []
    completed_map_jobs = set()
    current_map_jobs = set()
    pending_map_jobs = set()

    pending_reduce_jobs = set()
    completed_reduce_jobs = set()

    busy_workers = set()
    idle_workers = set()


    def __init__(self):
        self.reset_for_new_job()
        # gevent.spawn(self.controller)
        gevent.spawn(self.validate_workers)
        pass

    def reset_for_new_job(self):
        Master.job_name = None
        Master.input_file = None
        Master.chunk_size = None
        Master.no_of_reducers = 0
        Master.progress = "";

        for key in Master.workers_info.keys():
            Master.workers_info[key]["compeleted_jobs"] = set()
            Master.workers_info[key]["current_map_job"] = None

        Master.split_map_jobs = []
        Master.completed_map_jobs = set()
        Master.current_map_jobs = set()
        Master.pending_map_jobs = set()

        self.reset_reduce_phase()

    def process_heart_beat(self, ip_address, status, job_id, job_type):
        Master.progress = self.get_printable_status(status)
        print"[Heart_beat_Recieved from {0} ....  Status - {1}, Job_type - {2}, Job_ID - {3}......... ] \n".format(ip_address, self.get_printable_status(status), self.get_printable_status(job_type), job_id)
        self.register_worker(ip_address)


        if status == Config.WORKER_STATUS_IDLE or status == Config.WORKER_STATUS_COMPLETE:
            if status == Config.WORKER_STATUS_COMPLETE or status == Config.WORKER_STATUS_IDLE:
                if job_type == Config.WORKER_STATUS_WORKING_MAP and job_id is not None:
                    self.update_completed_map_jobs(ip_address, job_id)
                elif job_type == Config.WORKER_STATUS_WORKING_REDUCE and job_id is not None:
                    self.update_completed_reduce_jobs(ip_address, job_id)
                    
            if status == Config.WORKER_STATUS_REDUCE_FAILED:
                self.reset_reduce_phase()

            self.set_worker_idle(ip_address) # set the worker to Idle state

            if len(Master.pending_map_jobs) > 0:
                self.start_map_job()
            else:
                # print "******************************************* Completed Job {0} / Split Jobs {1} **************************************".format(len(Master.completed_map_jobs), len(Master.split_map_jobs))
                # missign_values = set(Master.split_map_jobs).difference(Master.completed_map_jobs)
                # print "******************************************* {0} ********************************************".format(str(missign_values))
                if len(Master.completed_map_jobs) == len(Master.split_map_jobs) and len(Master.pending_reduce_jobs) > 0:
                    self.start_reduce_job()
                else:
                    Master.idle_workers.add(ip_address)

    def get_printable_status(self, status):
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

    def reset_reduce_phase(self):
        Master.pending_reduce_jobs = set(list(range(0, Master.no_of_reducers)))
        Master.completed_reduce_jobs = set()

    def reset_failed_worker_job(self, worker_id):
        # Reset the completed Jobs
        for job_id in Master.workers_info[worker_id]["compeleted_jobs"]:
            Master.completed_map_jobs.discard(job_id)
            Master.pending_map_jobs.add(job_id)
        # Reset the current Map job
        current_job = Master.workers_info[worker_id]["current_map_job"]
        if current_job is not None:
            Master.pending_map_jobs.add(current_job)

        # Reset the current Reduce job
        current_job = Master.workers_info[worker_id]["current_reduce_job"]
        if current_job is not None:
            Master.pending_reduce_jobs.add(current_job)

    def update_completed_map_jobs(self, ip_address, job_id):
        if job_id != None:
            Master.workers_info[ip_address]["compeleted_jobs"].add(job_id)
            Master.workers_info[ip_address]["current_map_job"] = None
            Master.completed_map_jobs.add(job_id)
            Master.pending_map_jobs.discard(job_id)

    def update_completed_reduce_jobs(self, ip_address, job_id):
        if job_id != None:
            Master.workers_info[ip_address]["current_reduce_job"] = None
            Master.completed_reduce_jobs.add(job_id)
            Master.pending_reduce_jobs.discard(job_id)

    def validate_workers(self):
        # Check for worker heart beat Intervals.
        # Unregister invalid workers
        # reassign the job to the pending job pool
        while True:
            cur_time = time.time()
            for key in Master.workers_info.keys():
                value = Master.workers_info[key]
                last_time = value['last_received_hb']
                if cur_time - last_time > (Config.HEART_BEAT_TIME_INTREVAL * 3 + 1):
                    print '******************** Worker {0} is down ********************'.format(value['id'])
                    # cur_job_id = value['cur_job']
                    # if not cur_job_id is None:
                    #     assert(Master.jobId_state_map.has_key(cur_job_id))
                    #     cur_job = Master.jobId_state_map[cur_job_id]
                    #     Master.map_task_queue.put(cur_job)
                    # finished_job_id_list = value['finished_jobs']
                    # for id in finished_job_id_list.keys():
                    #     assert(Master.jobId_state_map.has_key(id))
                    #     job = Master.jobId_state_map[id]
                    #     if job['state'] == 'finished':
                    #         Master.finished_map_jobs.pop(job['job_id'], None)
                    #         job['state'] = 'ready'
                    #     Master.map_task_queue.put(job)
                    self.unregister_worker(key)

            gevent.sleep(Config.HEART_BEAT_TIME_INTREVAL)

    def register_worker(self, ip_address):
        cur_time = time.time()
        if ip_address in Master.workers_info:
            Master.workers_info[ip_address]['last_received_hb'] = cur_time
            return
        Master.workers_info[ip_address] = {"id": ip_address, "current_map_job": None, "compeleted_jobs": set(), "last_received_hb": cur_time, "current_reduce_job": None}
        Master.all_workers.add(ip_address)
        self.set_worker_idle(ip_address)
        print "[New worker {0} Registered .... ]".format(ip_address)

    def unregister_worker(self, ip_address):
        self.reset_failed_worker_job(ip_address)
        del Master.workers_info[ip_address]
        Master.all_workers.discard(ip_address)

    def start_map_job(self):
        self.reset_reduce_phase()
        worker_id = Master.idle_workers.pop()
        job_name = Master.job_name
        input_file = Master.input_file
        start_index = Master.pending_map_jobs.pop()
        chunk_size = Master.chunk_size
        c = zerorpc.Client()
        c.connect(worker_id)
        print "[......... Starting {1} Job On {0} ..... (Map Phase) ......] \n".format(worker_id, job_name)

        if c.start_map_job(job_name, input_file, start_index, chunk_size, Master.no_of_reducers):
            print "[......... {1} Job started successfully On  {0} ..... Job_id - {2} (Map Phase).......] \n".format(worker_id, job_name, start_index)
            Master.workers_info[worker_id]["current_map_job"] = start_index
            self.set_worker_busy(worker_id)
        else:
            print "[.............. Unable to Start {1} Job On  {0} ..... (Map Phase)..........] \n".format(worker_id, job_name)
            Master.workers_info[worker_id]["current_map_job"] = None
            Master.pending_map_jobs.add(start_index)
            self.set_worker_idle(worker_id)

        c.close()

    def start_reduce_job(self):
        worker_id = Master.idle_workers.pop()
        job_name = Master.job_name
        reducer_id = Master.pending_reduce_jobs.pop()
        c = zerorpc.Client()
        c.connect(worker_id)
        print "[...... Starting {1} Job On {0} ..... (Reduce Phase) ...........] \n".format(worker_id, job_name)

        if c.start_reduce_job(reducer_id, self.get_other_workers(worker_id)):
            print "[......... {1} Job started successfully On  {0} ..... {1} (Reduce Phase) ........] \n".format(worker_id, job_name, reducer_id)
            Master.workers_info[worker_id]["current_reduce_job"] = reducer_id
            self.set_worker_busy(worker_id)
        else:
            print "[........... Unable to Start {1} Job On  {0} ..... (Reduce Phase)........ ] \n".format(worker_id, job_name)
            Master.pending_reduce_jobs.add(reducer_id)
            self.set_worker_idle(worker_id)

        c.close()

    def set_worker_busy(self, ip_address):
        Master.idle_workers.discard(ip_address)
        Master.busy_workers.add(ip_address)


    def set_worker_idle(self, ip_address):
        Master.busy_workers.discard(ip_address)
        Master.idle_workers.add(ip_address)
        Master.workers_info[ip_address]["current_reduce_job"] = None
        Master.workers_info[ip_address]["current_map_job"] = None

    def split_input(self, input_file, chunk_size):
        return splitter.split_file(input_file, chunk_size)

    def submit_job(self, job_name, input_file, chunk_size, no_of_reducers):
        self.reset_for_new_job()
        Master.no_of_reducers = no_of_reducers
        Master.progress = "Submiting Job ....."
        Master.job_name = job_name
        Master.input_file = input_file
        Master.chunk_size = chunk_size
        Master.split_map_jobs = self.split_input(input_file, chunk_size)
        Master.pending_map_jobs = set(Master.split_map_jobs)
        Master.pending_reduce_jobs = set(list(range(0, no_of_reducers)))
        print "[{0} Job Submmited ..... ]".format(job_name)

    def get_job_id(self, job):
        return Master.split_map_jobs.index(job)

    def get_other_workers(self, ip_address):
        other_workers = []
        for worker_id in Master.all_workers:
            if worker_id != ip_address:
                other_workers.append(worker_id)
        return other_workers

    def progress_update(self):
        percent_remaining= 100
        if Master.job_name !=None and len(Master.pending_map_jobs) <= 0 and len(Master.pending_reduce_jobs) <= 0 and len(Master.completed_reduce_jobs) == Master.no_of_reducers:
            self.reset_for_new_job()
            print "........ Job Completed  ...... "
            for worker_id in Master.all_workers:
                c = zerorpc.Client()
                c.connect(worker_id)
                c.reset_worker()
                c.close()
            return "Completed"

        if(len(Master.split_map_jobs) > 0):
            total_jobs = len(Master.split_map_jobs) + Master.no_of_reducers
            total_complete_jobs = (len(Master.completed_map_jobs)+len(Master.completed_reduce_jobs))
            percent_remaining = (total_complete_jobs/total_jobs) *100
            return "...Working on {0} Job ..... Completed {2}/{1} ....".format(Master.job_name, total_jobs, total_complete_jobs)
            percent_complete = 100 - percent_remaining
            # return "Working on {0} Job , Remaining - {1} % ..... ".format(Master.job_name, percent_remaining)
        else:
            return "Wating For Jobs... !!"



s = zerorpc.Server(Master())
print(s.bind(Config.MASTER_IP))
s.run()