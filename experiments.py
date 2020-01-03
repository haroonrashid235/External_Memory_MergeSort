from programs import file_length_byte_stream, file_length_buffered_stream, file_length_mmap_stream
from programs import rand_jump_byte_stream, rand_jump_buffered_stream, rand_jump_mmap_stream

import os


def benchmark_sequential_reading(stream_type, filename, buffer_size = None, times=5):
    assert isinstance(stream_type, str)
    assert stream_type in ['byte','buffer','mmap']

    if stream_type == 'byte':
        print(f"\nByte Stream: {filename}\t Total Iterations:{times}")
        total_time = 0
        for i in range(times):
            file_sum, time_taken = file_length_byte_stream(filename)
            total_time += time_taken
        avg_time = total_time / times
        return file_sum, avg_time

    elif stream_type == 'mmap':
        print(f"\nMemoryMapped Stream: {filename}\t Buffer Size: {buffer_size}\t Total Iterations:{times}")
        total_time = 0
        for i in range(times):
            file_sum, time_taken = file_length_mmap_stream(filename, buffer_size)
            total_time += time_taken
        avg_time = total_time / times
        return file_sum, avg_time

    else:
        print(f"\nBuffered Stream: {filename}\t Buffer Size: {buffer_size}\t Total Iterations:{times}")
        total_time = 0
        for i in range(times):
            file_sum, time_taken = file_length_buffered_stream(filename, buffer_size)
            total_time += time_taken
        avg_time = total_time / times
        return file_sum, avg_time


def benchmark_random_reading(stream_type, filename, j = 10000, buffer_size = None, times=5):
    assert isinstance(stream_type, str)
    assert stream_type in ['byte','buffer','mmap']

    if stream_type == 'byte':
        print(f"\nByte Stream: {filename}\t Jumps: {j}\tTotal Iterations:{times}")
        total_time = 0
        for i in range(times):
            file_sum, time_taken = rand_jump_byte_stream(filename, j)
            total_time += time_taken
        avg_time = total_time / times
        return file_sum, avg_time

    elif stream_type == 'mmap':
        print(f"\nMemoryMapped Stream: {filename}\t Buffer Size: {buffer_size}\t Jumps: {j}\tTotal Iterations:{times}")
        total_time = 0
        for i in range(times):
            file_sum, time_taken = rand_jump_mmap_stream(filename, j, buffer_size)
            total_time += time_taken
        avg_time = total_time / times
        return file_sum, avg_time

    else:
        print(f"\nBuffered Stream: {filename}\t Buffer Size: {buffer_size}\t Jumps: {j}\tTotal Iterations:{times}")
        total_time = 0
        for i in range(times):
            file_sum, time_taken = rand_jump_buffered_stream(filename, j, buffer_size)
            total_time += time_taken
        avg_time = total_time / times
        return file_sum, avg_time
