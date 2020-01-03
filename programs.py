from stream import ByteInputStream, BufferedInputStream, BufferedOutputStream, MemMappedInputStream, MemMappedOutputStream

import os
import time
import random

        
def file_length_byte_stream(filename):
    sum = 0
    input_stream = ByteInputStream(filename)
    input_stream.open()
    start_time = time.time()
    while not input_stream.end_of_stream():
        sum += len(input_stream.read_line())
    end_time = time.time()
    return sum, end_time - start_time


def file_length_buffered_stream(filename, buffer_size = None):
    sum = 0
    input_stream = BufferedInputStream(filename, buffer_size)
    input_stream.open()
    start_time = time.time()
    for line in input_stream.read_lines():
        sum += len(line)
    end_time = time.time()
    return sum, end_time - start_time


def file_length_mmap_stream(filename, buffer_size):
    sum = 0
    input_stream = MemMappedInputStream(filename, buffer_size)
    input_stream.open()
    start_time = time.time()
    while not input_stream.end_of_stream():
        sum += len(input_stream.read_line())
    end_time = time.time()
    return sum, end_time - start_time


def rand_jump_byte_stream(filename, j, buffer_size=None):
    file_size = os.path.getsize(filename)
    input_stream = ByteInputStream(filename)
    input_stream.open()
    sum = 0
    start_time = time.time()
    for i in range(j):
        p = random.randint(1,file_size)
        input_stream.seek(p)
        sum += len(input_stream.read_line())
    end_time = time.time()
    return sum, end_time - start_time


def rand_jump_buffered_stream(filename, j, buffer_size=None):
    file_size = os.path.getsize(filename)
    input_stream = BufferedInputStream(filename, buffer_size)
    input_stream.open()
    sum = 0
    start_time = time.time()
    for i in range(j):
        p = random.randint(1,file_size)
        input_stream.seek(p)
        sum += len(input_stream.read_line())
    end_time = time.time()
    return sum, end_time - start_time


def rand_jump_mmap_stream(filename, j, buffer_size):
    file_size = os.path.getsize(filename)
    input_stream = MemMappedInputStream(filename, buffer_size)
    input_stream.open()
    sum = 0
    start_time = time.time()
    for i in range(j):
        p = random.randint(1,file_size)
        input_stream.seek(p)
        sum += len(input_stream.read_line())
    end_time = time.time()
    return sum, end_time - start_time



def rrmerge(files_list, target_file):
    assert isinstance(files_list, list)
    input_streams = []
    
    for filename in files_list:
        input_stream = BufferedInputStream(filename)
        input_stream.open()
        input_streams.append(input_stream)

    output_stream = BufferedOutputStream(target_file)
    output_stream.create()
    
    total_streams = len(input_streams)
    count = 0
    while True:
        for i_stream in input_streams:
            line = i_stream.read_line()
            output_stream.write_line(line)

            if i_stream.end_of_stream():
                i_stream.close()
                input_streams.remove(i_stream)
        count += 1
        if count % 10000 == 0:
            print(f"Lines Written: {count}\tStreams in progress: {len(input_streams)}/{total_streams}")

        if not len(input_streams):
            break
    output_stream.close()
    print('Merged Files...')