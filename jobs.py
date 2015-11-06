import sys
import mapreduce


class Jobs(object):
    WORD_COUNT_JOB = "WordCount"
    HAMMING_ENCODE_JOB = "HE"
    HAMMING_DECODE_JOB = "HD"
    HAMMING_FIX_JOB = "HF"
    HAMMING_CHECK_JOB = "HC"
    HAMMING_ERROR_JOB = "HEr"
    SORTING_JOB = "Sorting"


class WordCountMap(mapreduce.Map):
    def map(self, k, v):
        words = v.split()
        for w in words:
            self.emit(w, '1')


class WordCountReduce(mapreduce.Reduce):
    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            count = count + int(v)
        self.emit(k + ':' + str(count))


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


import random
import hamming_mapreduce
import splitter


# generate parity
def generate_parity(byte_string, indicies):
    sum = ''
    for i in indicies:
        sum += byte_string[i]
    return str(str.count(sum, '1') % 2)


def delete_parity(byte_string):
    return byte_string[2] + byte_string[4:7] + byte_string[8:12]


def get_ascii(byte_str):
    """Get ASCII character from a binary string."""
    data = byte_str[0:8]
    value = int(data, 2)
    return chr(value)


# extend 8bit to 12bit with 0
def extend_to_hanming(byte_string):
    tempory_list = list(byte_string)
    tempory_list.insert(0, '0')
    tempory_list.insert(1, '0')
    tempory_list.insert(3, '0')
    tempory_list.insert(7, '0')
    return ''.join(tempory_list)


class HammingEncodingMap(hamming_mapreduce.Map):
    def encode_map(self, k, byte_str):
        hamming_str = list(extend_to_hanming(byte_str))
        hamming_str[0] = str(generate_parity(hamming_str, [2, 4, 6, 8, 10]))
        hamming_str[1] = str(generate_parity(hamming_str, [2, 5, 6, 9, 10]))
        hamming_str[3] = str(generate_parity(hamming_str, [4, 5, 6, 11]))
        hamming_str[7] = str(generate_parity(hamming_str, [8, 9, 10, 11]))
        res = ''.join(hamming_str)
        self.emit(k, res)


class HammingEncodingReduce(hamming_mapreduce.Reduce):
    def reduce(self, k, vlist):
        res = ''.join(vlist)
        bin_text = ''
        for index in range(len(res) / 8):
            substring = res[index * 8: index * 8 + 8]
            byteval = int(substring, base=2)
            bin_text += (chr(byteval))
        result = list(bin_text)
        for v in result:
            self.emit(v)


class HammingDecodingMap(hamming_mapreduce.Map):
    def decode_map(self, k, hamming_code):
        if (len(hamming_code) != 12):
            return
        code = delete_parity(hamming_code)
        character = get_ascii(code)
        self.emit(k, character)


class HammingDecodingReduce(hamming_mapreduce.Reduce):
    def reduce(self, k, vlist):
        self.emit(''.join(vlist))


# class HammingCheckMap(hamming_mapreduce.Map):
#     def check_map(self, k, hamming_str):
#         err_position = 0
#         hamming_str = list(hamming_str)
#         chk0 = str(generate_parity(hamming_str, [2, 4, 6, 8, 10]))
#         chk1 = str(generate_parity(hamming_str, [2, 5, 6, 9, 10]))
#         chk3 = str(generate_parity(hamming_str, [4, 5, 6, 11]))
#         chk7 = str(generate_parity(hamming_str, [8, 9, 10, 11]))
#         if (chk0 != hamming_str[0]):
#             err_position += 1
#         if (chk1 != hamming_str[1]):
#             err_position += 2
#         if (chk3 != hamming_str[3]):
#             err_position += 4
#         if (chk7 != hamming_str[7]):
#             err_position += 8
#         if (err_position != 0):
#             print ("error_position " + str(err_position - 1), "1")
#             self.emit("error_position " + str(err_position - 1), "1")
#
#
# class HammingCheckReduce(hamming_mapreduce.Reduce):
#     def reduce(self, k, vlist):
#         count = 0
#         self.emit(k)

class HammingCheckMap(hamming_mapreduce.Map):
    def check_map(self, k, hamming_str):
        err_position = 0
        hamming_str = list(hamming_str)
        chk0 = str(generate_parity(hamming_str,[2,4,6,8,10]))
        chk1 = str(generate_parity(hamming_str,[2,5,6,9,10]))
        chk3 = str(generate_parity(hamming_str,[4,5,6,11]))
        chk7 = str(generate_parity(hamming_str,[8,9,10,11]))
        if(chk0 != hamming_str[0]):
            err_position += 1
        if(chk1 != hamming_str[1]):
            err_position += 2
        if(chk3 != hamming_str[3]):
            err_position += 4
        if(chk7 != hamming_str[7]):
            err_position += 8
        if(err_position != 0):
            print ("error_position",str(err_position-1))
            self.emit("error_position",str(err_position-1))

class HammingCheckReduce(hamming_mapreduce.Reduce):
    def reduce(self, k, vlist):
        count = 0
        for v in vlist:
            self.emit(k + ': ' + str(int(v)+count))
            count+=11


class HammingErrorMap(hamming_mapreduce.Map):
    def error_map(self, k, hamming_str):
        hamming = list(hamming_str)
        position = random.randint(0, len(hamming_str) - 1)
        print "Introduce error on position: ", position
        if (hamming[position] == '0'):
            hamming[position] = '1'
        else:
            hamming[position] = '0'

        self.emit(k, ''.join(hamming))


class HammingErrorReduce(hamming_mapreduce.Reduce):
    def reduce(self, k, vlist):
        res = ''.join(vlist)
        bin_text = ''
        for index in range(len(res) / 8):
            substring = res[index * 8: index * 8 + 8]
            byteval = int(substring, base=2)
            bin_text += (chr(byteval))
        result = list(bin_text)
        for v in result:
            self.emit(v)


class HammingFixMap(hamming_mapreduce.Map):
    def fix_map(self, k, hamming_str):
        err_position = 0
        hamming_str = list(hamming_str)
        chk0 = str(generate_parity(hamming_str, [2, 4, 6, 8, 10]))
        chk1 = str(generate_parity(hamming_str, [2, 5, 6, 9, 10]))
        chk3 = str(generate_parity(hamming_str, [4, 5, 6, 11]))
        chk7 = str(generate_parity(hamming_str, [8, 9, 10, 11]))
        if (chk0 != hamming_str[0]):
            err_position += 1
        if (chk1 != hamming_str[1]):
            err_position += 2
        if (chk3 != hamming_str[3]):
            err_position += 4
        if (chk7 != hamming_str[7]):
            err_position += 8
        if (err_position != 0):
            if (hamming_str[err_position - 1] == '0'):
                print "fix error on position: ", err_position
                hamming_str[err_position - 1] = '1'
            else:
                hamming_str[err_position - 1] = '0'
        self.emit(k, ''.join(hamming_str))


class HammingFixReduce(hamming_mapreduce.Reduce):
    def reduce(self, k, vlist):
        res = ''.join(vlist)
        bin_text = ''
        for index in range(len(res) / 8):
            substring = res[index * 8: index * 8 + 8]
            byteval = int(substring, base=2)
            bin_text += (chr(byteval))
        result = list(bin_text)
        for v in result:
            self.emit(v)
