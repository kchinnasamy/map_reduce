import sys
import os
import binascii

# define the function to split the file into smaller chunks


def split_file(inputFile,chunckSize):
    # get the length of data, ie size of the input file in bytes
    bytes = os.stat(inputFile).st_size
    chunk = int(chunckSize)

    #calculate the number of chunks to be created
    numChuncks= bytes/chunk
    if(bytes%chunk):
      numChuncks+=1

    offset = []
    for i in range(0, bytes+1, chunk):
        offset.append(i)


    return offset

def read_chunk_by_word(input_file, start_index, chunk_size):
    #TODO fix the word with a split
    bytes = os.stat(input_file).st_size
    start_index = int(start_index)
    end_index = start_index + int(chunk_size)
    if(end_index > bytes):
        end_index = bytes

    #fix the word with a split
    f = open(input_file,'rb')
    if(start_index != 0):
        f.seek(start_index)
        character = f.read(1)
        while(character!=' ' and character!= '\n'):
            if(start_index == 0):
                break
            start_index-=1
            f.seek(start_index)
            character = f.read(1)
        f.seek(start_index)
    f.seek(end_index)
    end_character = f.read(1)
    while(end_character!=' ' and end_character!='\n'):
    # always get rid of last word
        if(end_index == 0):
            break
        end_index-=1
        f.seek(end_index)
        end_character = f.read(1)
    f.seek(start_index)
    data = f.read(end_index-start_index)
    f.close()
    return data

def read_chunk(input_file, start_index, chunk_size):
    bytes = os.stat(input_file).st_size
    start_index = int(start_index)
    end_index = start_index + int(chunk_size)
    if(end_index > bytes):
        end_index = bytes
    f = open(input_file,'rb')
    f.seek(start_index)
    data = f.read(end_index - start_index)
    f.close()
    return data

def read_binary_chunk(input_file, start_index, chunk_size):
    bytes = os.stat(input_file).st_size
    start_index = int(start_index)
    end_index = start_index + int(chunk_size)
    if(end_index > bytes):
        end_index = bytes
    f = open(input_file,'rb')
    f.seek(start_index)
    text=''
    index = 0
    while index < int(end_index - start_index):
        b = f.read(1)
        if not b:
            break
        hexadecimal = binascii.hexlify(b)
        decimal = int(hexadecimal, 16)
        text+= bin(decimal)[2:].zfill(8)
        index+=1
    return text



if __name__ == '__main__':
    input = sys.argv[1]
    chunckSize = sys.argv[2]
    offset = split_file(input,chunckSize)
    print offset
    # offset = split_file(input,chunckSize)
    # print offset
    # print read_chunk(input, offset[int(sys.argv[3])], chunckSize)
    print read_binary_chunk(input, offset[int(sys.argv[3])], chunckSize)

