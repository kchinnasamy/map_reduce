# mapreduce.py
import gevent
from config import *
import collections
class Map(object):

    def __init__(self):
        self.table = {}

    def map(self, k, v):
        pass

    def emit(self, k, v):
        if k in self.table:
            self.table[k].append(v)
        else:
            self.table[k] = [v]

    def get_table(self):
        return self.table


class Partition(object):
    def __init__(self):
        pass

    def partition_function(self, mapper, total_reduces):
        total_reduces = int(total_reduces)
        list = []
        for i in range(0, total_reduces):
            list.append({})

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()

        for key in keys:
            reducer_id = hash(key) % total_reduces
            table_obj = list[reducer_id]
            if key in table_obj:
                table_obj[key].append(table[key])
            else:
                table_obj[key] = table[key]
            gevent.sleep(Config.MIN_TIME_TO_PREEMPT_TASK) #this is to ask the worker to wait to send a heart beat
        return list

class Reduce(object):

    def __init__(self):
        self.result_list = []

    def reduce(self, k, vlist):
       pass

    def emit(self, v):
        self.result_list.append(v)

    def get_result_list(self):
        return self.result_list


class Engine(object):

    # def __init__(self, input_list, map_class, reduce_class, total_reducers):
    #     self.input_list = input_list
    #     self.map_class = map_class
    #     self.reduce_class = reduce_class
    #     self.partition_class = None
    #     self.total_reducers = total_reducers
    #     self.result_list = None

    def __init__(self, input_list, map_class, reduce_class, partition_class, total_reducers):
        self.input_list = input_list # for some reason this need input as list of lines of words
        self.map_class = map_class
        self.reduce_class = reduce_class
        self.partition_class = partition_class
        self.total_reducers = total_reducers
        self.result_list = None


    def map_phase(self):
        # Map phase
        mapper = self.map_class()
        for i, v in enumerate(self.input_list.split("\n")):
            mapper.map(i, v)
            gevent.sleep(Config.MIN_TIME_TO_PREEMPT_TASK) #this is to ask the worker to wait to send a heart beat

        # Call the partition function
        # if self.partition_class is None:
        #     return self.partition_function(mapper, self.total_reducers)
        # else:
        return self.partition_class().partition_function(mapper, self.total_reducers)


    def reduce_phase(self, table):

        # Sort intermediate keys
        # table = mapper.get_table()
        keys = table.keys()
        keys.sort()
        # Reduce phase
        reducer = self.reduce_class()
        for k in keys:
            reducer.reduce(k, table[k])
            gevent.sleep(Config.MIN_TIME_TO_PREEMPT_TASK) #this is to ask the worker to wait to send a heart beat

        result_list = reducer.get_result_list()
        return result_list

    def get_result_list(self):
        return self.result_list

if __name__ == '__main__':
    values = ['foo', 'bar', 'baz']
    engine = Engine(values, Map, Reduce)
    print engine.reduce_phase(engine.map_phase())








