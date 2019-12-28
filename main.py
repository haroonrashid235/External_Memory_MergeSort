import os
import time
import random

from stream import ByteInputStream, BufferedInputStream
        
def file_length_byte_stream(filename):
    sum = 0
    input_stream = ByteInputStream(filename)
    input_stream.open()
    print(f"\nByte Stream: {filename}")
    start_time = time.time()
    while not input_stream.end_of_stream():
        sum += len(input_stream.read_line())
    end_time = time.time()
    return sum, end_time - start_time


def file_length_buffered_stream(filename, buffer_size=None):
    sum = 0
    input_stream = BufferedInputStream(filename, buffer_size)
    input_stream.open()
    print(f"\nBuffered Stream: {filename}\t Buffer Size: {buffer_size}")
    start_time = time.time()
    for line in input_stream.read_lines():
        sum += len(line)
    end_time = time.time()
    return sum, end_time - start_time

def rand_jump_byte_stream(filename, j, buffer_size=None):
    file_size = os.path.getsize(filename)
    input_stream = ByteInputStream(filename)
    input_stream.open()
    sum = 0
    start_time = time.time()
    print(f"\nByte Stream: {filename}\t Jumps: {j}")
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
    print(f"\nBuffered Stream: {filename}\tJumps: {j}\tBuffer Size: {buffer_size}")
    for i in range(j):
        p = random.randint(1,file_size)
        input_stream.seek(p)
        sum += len(next(input_stream.read_lines()))
    end_time = time.time()
    return sum, end_time - start_time

# TEST CODE
FILE_NAME = 'data/company_name.csv'
# Sequential Reading using Different Read Streams
print("Exp 1.1: SEQUENTIAL READING...")
file_sum, time_taken = file_length_byte_stream(FILE_NAME)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = file_length_buffered_stream(FILE_NAME)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = file_length_buffered_stream(FILE_NAME,buffer_size=5)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = file_length_buffered_stream(FILE_NAME,buffer_size=50)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = file_length_buffered_stream(FILE_NAME,buffer_size=500)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = file_length_buffered_stream(FILE_NAME,buffer_size=5000)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")



# Random Reading using Different Read Streams
print("\nExp 1.2: RANDOM READING...")
file_sum, time_taken = rand_jump_byte_stream(FILE_NAME, 10000)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = rand_jump_buffered_stream(FILE_NAME, j=10000)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = rand_jump_buffered_stream(FILE_NAME,j=10000, buffer_size=5)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = rand_jump_buffered_stream(FILE_NAME,j=10000, buffer_size=50)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = rand_jump_buffered_stream(FILE_NAME,j=10000, buffer_size=500)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")
file_sum, time_taken = rand_jump_buffered_stream(FILE_NAME,j=10000, buffer_size= 5000)
print(f"File Length: {file_sum}\t\tTime {round(time_taken * 1000,4)}ms")


