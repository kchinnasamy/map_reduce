import sys
import mapreduce
import splitter

class SortingMap(mapreduce.Map):
    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit('1', w)


class SortingReduce(mapreduce.Reduce):
    def reduce(self, k, vlist):
        vlist.sort()
        for v in vlist:
            self.emit(v)


if __name__ == '__main__':

    # f = open("sort_text.txt")
    # values = f.readlines()
    # f.close()
    values = splitter.read_chunk("wordcount.py", 1, 100)


    engine = mapreduce.Engine(values, SortingMap, SortingReduce, mapreduce.Partition, 2)
    map_output = engine.map_phase()

    for partition in map_output:
        result_list = engine.reduce_phase(partition);
        for r in result_list:
            print r
        print "------------------------------------------------------------------------------------------"
