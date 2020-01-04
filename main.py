import os
import mmap

from programs import file_length_byte_stream, file_length_buffered_stream
from programs import rand_jump_byte_stream, rand_jump_buffered_stream   
import experiments 

data_folder = 'data'
FILE_NAME = 'data/company_name.csv'
NUM_TIMES = 3
data_files = os.listdir(data_folder)
log_folder = 'logs'
os.makedirs(log_folder, exist_ok=True)

test_files = []
for file in data_files:
    file_path = os.path.join(data_folder, file)
    file_size = os.path.getsize(file_path)
    test_files.append((file_path, file_size))

test_files.sort(key = lambda x:x[1])
# Sequential Reading using Different Read Streams

def run_experiment_1(test_files):
    print("Exp 1.1: SEQUENTIAL READING...")
    log_string = "file_path,file_size,stream_type,file_sum,avg_time\n"
    for file_path, file_size in test_files:
        if file_size > 10000000:
            log_string += f"{file_path},{file_size},byte_stream,,\n"
            continue
        file_sum, avg_time = experiments.benchmark_sequential_reading(stream_type='byte', filename=file_path, times=NUM_TIMES)
        print(f"FILE_SUM: {file_sum}\t\tAVG_TIME: {round(avg_time * 1000,4)}ms")
        log_string += f"{file_path},{file_size},byte_stream,{file_sum},{round(avg_time*1000,4)}\n"    

    with open(os.path.join(log_folder,'log_byte_stream_sequential.csv'), 'w') as f:
        f.write(log_string)

    buffer_sizes = [None, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 12288, 16384, 20480]
    for buffer_size in buffer_sizes:
        log_string = "file_path,file_size,stream_type,buffer_size,file_sum,avg_time\n"
        for file_path, file_size in test_files:
            file_sum, avg_time = experiments.benchmark_sequential_reading(stream_type='buffer', 
                filename=file_path, buffer_size=buffer_size, times=NUM_TIMES)
            print(f"FILE_SUM: {file_sum}\t\tAVG_TIME: {round(avg_time * 1000,4)}ms")
            log_string += f"{file_path},{file_size},buffered_stream,{buffer_size},{file_sum},{round(avg_time*1000,4)}\n"

        if buffer_size is None:
            log_file_name = os.path.join(log_folder,f'log_buffered_sequential.csv')
        else:
            log_file_name = os.path.join(log_folder,f'log_buffered_sequential_{buffer_size}.csv')
        with open(log_file_name, 'w') as f:
            f.write(log_string)

    buffer_sizes = [8192, 12288, 16384, 20480]
    for buffer_size in buffer_sizes:
        log_string = "file_path,file_size,stream_type,buffer_size,file_sum,avg_time\n"
        for file_path, file_size in test_files:
            file_sum, avg_time = experiments.benchmark_sequential_reading(stream_type='mmap', 
                filename=file_path, buffer_size=buffer_size, times=NUM_TIMES)
            print(f"FILE_SUM: {file_sum}\t\tAVG_TIME: {round(avg_time * 1000,4)}ms")
            log_string += f"{file_path},{file_size},mmap_stream,{buffer_size},{file_sum},{round(avg_time*1000,4)}\n"

        log_file_name = os.path.join(log_folder,f'log_mmap_sequential_{buffer_size}.csv')
        with open(log_file_name, 'w') as f:
            f.write(log_string)

# Random Reading using Different Read Streams
def run_experiment_2(test_files):

    print("\n\nExp 1.2: RANDOM READING...")

    # Benchmark Byte Stream
    random_reads = [100, 1000, 10000]
    log_string = "file_path,file_size,stream_type,file_sum,avg_time_100,avg_time_1000,avg_time_10000\n"
    for file_path, file_size in test_files:
        times = []
        for j in random_reads:
            file_sum, avg_time = experiments.benchmark_random_reading(stream_type='byte', j=j, filename=file_path, times=NUM_TIMES)
            print(f"FILE_SUM: {file_sum}\tNUM_READS: {j}\tAVG_TIME: {round(avg_time * 1000,4)}ms")
            times.append(round(avg_time*1000,4))
        log_string += f"{file_path},{file_size},byte_stream,{file_sum},{times[0]},{times[1]},{times[2]}\n"

        with open(os.path.join(log_folder,'log_byte_random.csv'), 'w') as f:
            f.write(log_string)



    # Benchmark Buffered Stream
    buffer_sizes = [None, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 12288, 16384, 20480]
    for buffer_size in buffer_sizes:
        log_string = "file_path,file_size,stream_type,buffer_size,file_sum,avg_time_100,avg_time_1000,avg_time_10000\n"
        for file_path, file_size in test_files:
            times = []
            for j in random_reads:
                file_sum, avg_time = experiments.benchmark_random_reading(stream_type='buffer',j=j, 
                    filename=file_path, buffer_size=buffer_size, times=NUM_TIMES)
                print(f"FILE_SUM: {file_sum}\tNUM_READS:{j}\tAVG_TIME {round(avg_time * 1000,4)}ms")
                times.append(round(avg_time*1000,4))
            log_string += f"{file_path},{file_size},buffered_stream,{buffer_size},{file_sum},{times[0]},{times[1]},{times[2]}\n"

        if buffer_size is None:
            log_file_name = os.path.join(log_folder,f'log_buffered_random.csv')
        else:
            log_file_name = os.path.join(log_folder,f'log_buffered_random_{buffer_size}.csv')
        with open(log_file_name, 'w') as f:
            f.write(log_string)


    buffer_sizes = [8192, 12288, 16384, 20480]
    for buffer_size in buffer_sizes:
        log_string = "file_path,file_size,stream_type,buffer_size,file_sum,avg_time_100,avg_time_1000,avg_time_10000\n"
        for file_path, file_size in test_files:
            times = []
            for j in random_reads:
                file_sum, avg_time = experiments.benchmark_random_reading(stream_type='mmap',j=j, 
                    filename=file_path, buffer_size=buffer_size, times=NUM_TIMES)
                print(f"FILE_SUM: {file_sum}\tNUM_READS:{j}\tAVG_TIME: {round(avg_time * 1000,4)}ms")
                times.append(round(avg_time*1000,4))
            log_string += f"{file_path},{file_size},mmap_stream,{buffer_size},{file_sum},{times[0]},{times[1]},{times[2]}\n"

        if buffer_size is None:
            log_file_name = os.path.join(log_folder,f'log_mmap_random.csv')
        else:
            log_file_name = os.path.join(log_folder,f'log_mmap_random_{buffer_size}.csv')
        with open(log_file_name, 'w') as f:
            f.write(log_string)

def run_experiment3():
    print("Exp 1.3: Combined Reading and Writing...")
    io_pairs = [('buffer','byte'),('buffer','buffer'),('mmap','byte'),('mmap','buffer')]
    file_names = os.listdir('data')
    files_list = [os.path.join('data', x) for x in file_names if '.csv' in x and x[0] != '.']
    files_list = [x for x,y in test_files if y < 1000000000]
    
    target_folder = 'output'
    buffer_sizes = [8192, 12288, 16384]
    files_list = files_list[:10]
    for pair in io_pairs:
        log_string = "read_stream,write_stream,num_files,buffer_size,avg_time\n"
        for buffer_size in buffer_sizes:
            avg_time = experiments.benchmark_combined_read_write(pair[0], pair[1], files_list, target_folder, buffer_size=buffer_size, times=1)
            print(f"Pair: {pair}\tAvg Time {round(avg_time * 1000,4)}ms")
            log_string += f"{pair[0]},{pair[1]},{len(files_list)},{buffer_size},{round(avg_time * 1000,4)}\n"
        log_file_name = os.path.join(log_folder, f"log_rrmerge_{pair[0]}_{pair[1]}.csv")
        with open(log_file_name, 'w') as f:
                f.write(log_string)
                f.write(str(files_list) + '\n')


# run_experiment_1(test_files)
# run_experiment_2(test_files)
run_experiment3()



# TEST CODE
