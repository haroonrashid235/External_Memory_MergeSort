import os
import mmap

from programs import file_length_byte_stream, file_length_buffered_stream
from programs import rand_jump_byte_stream, rand_jump_buffered_stream   
import experiments 

data_folder = 'data'
FILE_NAME = 'data/company_name.csv'
NUM_TIMES = 5
buffer_sizes = [None, 4096*2, 4096*3, 4096*4]
data_files = os.listdir(data_folder)

test_files = []
for file in data_files:
    file_path = os.path.join(data_folder, file)
    file_size = os.path.getsize(file_path)
    test_files.append((file_path, file_size))

test_files.sort(key = lambda x:x[1])

# Sequential Reading using Different Read Streams
print("Exp 1.1: SEQUENTIAL READING...")
file_sum, avg_time = experiments.benchmark_sequential_reading(stream_type='byte', filename=FILE_NAME, times=NUM_TIMES)
print(f"File Length: {file_sum}\t\tAvg Time {round(avg_time * 1000,4)}ms")

for buffer_size in buffer_sizes:
    file_sum, avg_time = experiments.benchmark_sequential_reading(stream_type='buffer', 
        filename=FILE_NAME, buffer_size=buffer_size, times=NUM_TIMES)
    print(f"File Length: {file_sum}\t\tAvg Time {round(avg_time * 1000,4)}ms")


for buffer_size in buffer_sizes:
    if buffer_size is None:
        continue
    file_sum, avg_time = experiments.benchmark_sequential_reading(stream_type='mmap', 
        filename=FILE_NAME, buffer_size=buffer_size, times=NUM_TIMES)
    print(f"File Length: {file_sum}\t\tAvg Time {round(avg_time * 1000,4)}ms")

# Random Reading using Different Read Streams
print("\n\nExp 1.2: RANDOM READING...")

# Benchmark Byte Stream
file_sum, avg_time = experiments.benchmark_random_reading(stream_type='byte', j=1000, filename=FILE_NAME, times=NUM_TIMES)
print(f"File Length: {file_sum}\t\tAvg Time {round(avg_time * 1000,4)}ms")

# Benchmark Buffered Stream
for buffer_size in buffer_sizes:
    file_sum, avg_time = experiments.benchmark_random_reading(stream_type='buffer', 
        filename=FILE_NAME, j=1000, buffer_size=buffer_size, times=NUM_TIMES)
    print(f"File Length: {file_sum}\t\tAvg Time {round(avg_time * 1000,4)}ms")

# Benchmark MemMapped Stream
for buffer_size in buffer_sizes:
    if buffer_size is None:
        continue
    file_sum, avg_time = experiments.benchmark_random_reading(stream_type='mmap', 
        filename=FILE_NAME, j=1000, buffer_size=buffer_size, times=NUM_TIMES)
    print(f"File Length: {file_sum}\t\tAvg Time {round(avg_time * 1000,4)}ms")

# TEST CODE
file_names = os.listdir('data')

files_list = [os.path.join('data', x) for x in file_names if '.csv' in x and x[0] != '.']
# print(files_list)
# target_file = 'test.csv'
# rrmerge(files_list, target_file)
# assert False
