import os
import shutil
import queue
from datetime import datetime
import time
import pandas as pd

class ExtSort:

    """This file contains method for sorting the given file on Kth column under some constrains"""
    def __init__(self, file_name, k, m, d):
        self.file_name = file_name
        self.colNum = k
        self.buffer_size = m
        self.num_merge_stream = d
        self.counter = 0

    def sort(self):
        if self.num_merge_stream < 2:
            print("Merge stream should be greater or equal 2")
            return False
        self.file_queue = self.subdivide()

        while self.file_queue.qsize() > 1:
            # print(f"File Queue: {list(self.file_queue.queue)}")
            input_stream_ref = []
            for i in range(self.num_merge_stream):
                if not self.file_queue.empty():
                    input_stream_ref.append(self.file_queue.get())
                else:
                    break
            # print(f"Input stream reference: {input_stream_ref}")
            tmp_sorted_file_path = self.merge(input_stream_ref)
            self.file_queue.put(tmp_sorted_file_path)

        # print('Final Sorted File Path: ', self.file_queue.get())
        self.sorted_file_path = self.file_queue.get()
        # print(self.sorted_file_path)
        return

    def subdivide(self):
        file_queue = queue.Queue()

        #open the file in readmode
        self.input_file = open(self.file_name, mode="r", encoding="utf8")
        records = []
        remaining_length = self.buffer_size
        file_counter = 1

        #create a temporary folder for holding the files, delete the previous one
        if os.path.exists('temp'):
            shutil.rmtree('temp')
        os.makedirs('temp')

        for line in self.input_file:

            if self.colNum > len(line.split(',')):
                print("Given sorting column is more than number of column in records")
                return False
            elif len(line) > self.buffer_size: # memory buffer size is too small
                print("Memory buffer size is smaller than particular record")
                return False
            elif len(line) <= remaining_length: #enough space in buffer
                records.append(line)
                remaining_length = remaining_length - len(line)
            else: #not enough space in buffer; write the collected records in a file
                file_path = "temp/" + str(file_counter)+".csv" # write the record in a file
                print("File path: ", file_path)
                file_counter += 1

                #sort the record based on the given columns
                column_value = records[0].split(',')[int(self.colNum) - 1]
                # check if the column is integer, then correct sorting is applied
                if column_value.rstrip().isdigit():
                    sorted_record = sorted(records, key=lambda x: int(x.split(',')[int(self.colNum) - 1]))
                else:
                    sorted_record = sorted(records, key=lambda x: x.split(',')[int(self.colNum) - 1])
                #write the sorted records into file
                self.write_records(file_path, sorted_record)
                #put the file reference in the queue
                file_queue.put(file_path)
                records.clear()
                remaining_length = self.buffer_size
                records.append(line)
                remaining_length = remaining_length - len(line)
        return file_queue

    def write_records(self, file_path, record_list):
        file = open(file_path, 'w+', encoding="utf8")
        file.writelines(record_list)
        file.close()

    def merge(self, file_list):
        print(f"Merging {len(file_list)} files")
        # print(file_list)

        sort_column = self.colNum
        file_object_list = []
        for i in range(len(file_list)):
            ref = open(file_list[i], mode="r", encoding="utf8")
            file_object_list.append(ref)

        # priority queue to store one record from each stream
        record_priority_queue = queue.PriorityQueue()

        # add one record from each of the streams into queue
        for i in range(0, len(file_object_list)):
            f_object = file_object_list[i]
            record = f_object.readline()  # read a record from file
            if record:  # record not empty
                temp_pq_record = []
                col_value = record.split(',')[int(sort_column) - 1]

                if col_value.rstrip().isdigit():  # if column is a number, put it as number so that correct sorting is applied
                    col_value = int(col_value)
                temp_pq_record.append(col_value)  # key of the priority queue is going to be the column value
                temp_pq_record.append(i)  # need to track from which stream this record comes
                temp_pq_record.append(record)  # record will be the data for key in priority queue
                # print("Temp Record: ", temp_pq_record)
                record_priority_queue.put(temp_pq_record)

        # print("Size of the queue initially: ", record_priority_queue.qsize())

        # creating the file path for partially sorted streams and opening a file to store them
        current_time = datetime.now().strftime('%H_%M_%S')
        os.makedirs('temp2', exist_ok=True)
        output_file_path = "temp2/part_sort_" + str(self.counter) + ".csv"
        self.counter += 1
        out_stream = open(output_file_path, 'w+', encoding="utf8")

        while not record_priority_queue.empty():
            # get the data from queue and put in the out file
            data_item = record_priority_queue.get()
            file_obj_index = data_item[1]  # getting the stream number
            data = data_item[2]  # getting the record
            # print(repr(data))
            out_stream.write(data)  # put data into output stream
            # push new record from the same stream into queue
            record = file_object_list[file_obj_index].readline()
            if record:  # the stream has some record
                temp_pq_record = []
                # print("Inside: temp record before ", temp_pq_record)
                col_value = record.split(',')[int(sort_column) - 1]
                if col_value.rstrip().isdigit():  # if column is a number, put it as number so that correct sorting is applied
                    col_value = int(col_value)
                temp_pq_record.append(col_value)  # key of the priority queue is going to be the column value
                temp_pq_record.append(file_obj_index)  # need to track from which stream this record comes
                temp_pq_record.append(record)  # record will be the data fro key in priority queue
                # print("Inside: temp record after ", temp_pq_record)
                record_priority_queue.put(temp_pq_record)

        for obj in file_object_list:
            obj.close()
        # print("Size of priority queue at the end: ", record_priority_queue.qsize())
        out_stream.close()
        return output_file_path
