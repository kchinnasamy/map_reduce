import splitter
import sys, os
import mapreduce
import jobs

completed_jobs = {}


def shuffle_info(reducer_id, completed_map_jobs):
    map_data = {}
    for job_id in completed_map_jobs.keys():
        values = completed_map_jobs[job_id][reducer_id]
        map_data = shuffle_merge(map_data, values)
    return map_data


def shuffle_merge(data_1, data_2):
    for k in data_2.keys():
        if k in data_1:
            new_list = data_1[k] + data_2[k]
            data_1[k] = new_list
        else:
            data_1[k] = data_2[k]
    return data_1


job_name = sys.argv[1]
chunk_size = int(sys.argv[2])
no_of_reducers = int(sys.argv[3])
input_file = sys.argv[4]
output_file = sys.argv[5]
reducer_list = list(range(0, no_of_reducers))
splits = splitter.split_file(input_file, chunk_size)
engine = None


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        # if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
        pass  # re-raise exception if a different error occured


# Map Phase
for start_index in splits:
    if job_name == jobs.Jobs.SORTING_JOB or job_name == jobs.Jobs.WORD_COUNT_JOB:
        values = splitter.read_chunk_by_word(input_file, start_index, chunk_size)
    else:
        values = splitter.read_chunk(input_file, start_index, chunk_size)
    if (job_name == jobs.Jobs.WORD_COUNT_JOB):
        engine = mapreduce.Engine(values, jobs.WordCountMap, jobs.WordCountReduce, mapreduce.Partition, no_of_reducers)
    elif (job_name == jobs.Jobs.SORTING_JOB):
        engine = mapreduce.Engine(values, jobs.SortingMap, jobs.SortingReduce, mapreduce.Partition, no_of_reducers)

    map_output = engine.map_phase()
    completed_jobs[start_index] = map_output
    print "------------- Completed Map Job_ID: {0}--------------".format(start_index)

silentremove(output_file)

# Shuffle and Reduce Phase
for reducer_id in reducer_list:

    shuffle_data = shuffle_info(reducer_id, completed_jobs)
    print "----------------------------------------   Completed Shuffle for Reduce partition Job_Id: " + str(
        reducer_id) + "    --------------------------------------------------"
    result = engine.reduce_phase(shuffle_data)
    for r in result:
        with open(output_file, 'a') as f:
            f.write(str(r) + '\n')  # python will convert \n to os.linesep
    f.close()  # you can omit in most cases as the destructor will call it
    print "----------------------------------------   Completed Reduce Job_Id: " + str(
        reducer_id) + "    --------------------------------------------------"