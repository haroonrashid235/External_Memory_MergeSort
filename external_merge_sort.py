import os
import shutil
import queue

class extsort:

    """This file contains method for sorting the given file on Kth column under some constrains"""
    def __init__(self, file_name, k, m, d):
        self.file_name = file_name
        self.colNum = k
        self.buffer_size = m
        self.num_merge_stream = d

    def sort(self):

        if self.num_merge_stream < 2:
            print("Merge stream should be greater or equal 2.")
            return False

        self.file_queue = self.subdivide()

        if not self.file_queue:
            print("Sorting failed!")
            return False

        file_index = 1 #for naming the partialy sorted files
        while self.file_queue.qsize() > 1:
            input_stream_ref = []
            for i in range(0, self.num_merge_stream):
                if not self.file_queue.empty():
                    input_stream_ref.append(self.file_queue.get())
                else:
                    break
            tmp_sorted_file_path = self.merge(input_stream_ref, file_index)
            file_index = file_index + 1
            self.file_queue.put(tmp_sorted_file_path)

        self.sorted_file_path = self.file_queue.get()
        print("Final sorted file path: ", self.sorted_file_path)

    def subdivide(self):

        file_queue = queue.Queue()
        #open the file in readmode
        self.input_file=open(self.file_name, mode="r", encoding="utf8")

        #create a temporary folder for holding the files, delete the previous one
        if os.path.exists('temp'):
            shutil.rmtree('temp')
        os.makedirs('temp')
        #--------------------------------------------------------------------------------------------------------------

        records = []
        remaining_length = self.buffer_size
        file_counter = 1 #for naming the sorted files

        for line in self.input_file:

            if self.colNum > len(line.split(',')):
                print("Given sorting column is more than number of column in records or format error in file.")
                return False
            elif len(line) > self.buffer_size: #memory buffer size is too small
                print("Memory buffer size is smaller than particular record.")
                return False
            elif len(line) <= remaining_length: #enough space in buffer
                records.append(line)
                remaining_length = remaining_length - len(line)

            else: #not enough space in buffer; write the collected records in a file
                file_path = "temp/"+str(file_counter)+".csv" #write the record in a file
                file_counter = file_counter+1
                #sort the record based on the given columns
                #check if the column is integer, then correct sorting is applied
                if self.is_type_integer(self.file_name, self.colNum):
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

        if len(records) > 0: #write the final part of the segment
            file_path = "temp/" + str(file_counter) + ".csv"  # write the record in a file
            file_counter = file_counter + 1

            # sort the record based on the given columns
            if self.is_type_integer(self.file_name, self.colNum):
                sorted_record = sorted(records, key=lambda x: int(x.split(',')[int(self.colNum) - 1]))
            else:
                sorted_record = sorted(records, key=lambda x: x.split(',')[int(self.colNum) - 1])

            # write the sorted records into file
            self.write_records(file_path, sorted_record)
            # put the file reference in the queue
            file_queue.put(file_path)

        return file_queue

    def write_records(self, file_path, record_list):

        with open(file_path, 'w+', encoding="utf8") as file:
            file.writelines(record_list)
            file.close()

    def merge(self, file_list, num_index):

        sort_column = self.colNum
        file_object_list = []
        for i in range(0, len(file_list)):
            ref = open(file_list[i], mode="r", encoding="utf8")
            file_object_list.append(ref)

        # priority queue to store one record from each stream
        record_priority_queue = queue.PriorityQueue()

        # add one record from each of the streams into queue
        for i in range(0, len(file_object_list)):
            f_object = file_object_list[i]
            record = f_object.readline()  # read a record from file
            if record:  # recond not empty
                temp_pq_record = []

                col_value = record.split(',')[int(sort_column) - 1]
                if self.is_type_integer(self.file_name, self.colNum):  # if column is a number, put it as number so that correct sorting is applied
                    col_value = int(col_value)
                temp_pq_record.append(col_value)  # key of the priority queue is going to be the column value
                temp_pq_record.append(i)  # need to track from which stream this record comes
                temp_pq_record.append(record)  # record will be the data for key in priority queue
                record_priority_queue.put(temp_pq_record)

        # creating the file path for partially sorted streams and opening a file to store them
        output_file_path = "temp/part_sort_" + str(num_index) + ".csv"

        with open(output_file_path, 'w+', encoding="utf8") as out_stream:

            while not record_priority_queue.empty():
                # get the data from que and put in the out file
                data_item = record_priority_queue.get()
                file_obj_index = data_item[1]  # getting the stream number
                data = data_item[2]  # getting the record
                out_stream.write(data)  # put data into output stream

                # push new record from the same stream into queue
                record = file_object_list[file_obj_index].readline()
                if record:  # the stream has some record
                    temp_pq_record = []

                    col_value = record.split(',')[int(sort_column) - 1]

                    if self.is_type_integer(self.file_name, self.colNum):  # if column is a number, put it as number so that correct sorting is applied
                        col_value = int(col_value)
                    temp_pq_record.append(col_value)  # key of the priority queue is going to be the column value
                    temp_pq_record.append(file_obj_index)  # need to track from which stream this record comes
                    temp_pq_record.append(record)  # record will be the data fro key in priority queue
                    record_priority_queue.put(temp_pq_record)

            #close all the files opened for reading
            for obj in file_object_list:
                obj.close()
            os.fsync(out_stream.fileno())
            out_stream.close()
            return output_file_path

    def is_type_integer(self, file_name, column_number):
        if file_name == 'aka_name.csv':
            int_columns = [1, 2]
            if column_number in int_columns:
                return True
            else:
                return False
        elif file_name == 'aka_title.csv':
            int_columns = [1, 2, 5]
            if column_number in int_columns:
                return True
            else:
                return False
        elif file_name == 'cast_info.csv':
            int_columns = [1, 2, 3, 7]
            if column_number in int_columns:
                return True
            else:
                return False
        elif file_name == 'role_type.csv' or file_name == 'name.csv' or file_name == 'link_type.csv' or file_name == 'kind_type.csv' or file_name == 'keyword.csv' or file_name == 'info_type.csv' or file_name == 'char_name.csv' or file_name == 'comp_cast_type.csv' or file_name == 'company_name.csv' or file_name == 'company_type.csv':
            if column_number == 1:
                return True
            else:
                return False
        elif file_name == 'complete_cast.csv':
            int_columns = [1, 3, 4]
            if column_number in int_columns:
                return True
            else:
                return False
        elif file_name == 'movie_companies.csv' or file_name == 'movie_link.csv':
            int_columns = [1, 2, 3, 4]
            if column_number in int_columns:
                return True
            else:
                return False
        elif file_name == 'person_info.csv' or file_name == 'movie_info.csv' or file_name == 'movie_info_idx.csv' or file_name == 'movie_keyword.csv':
            int_columns = [1, 2, 3]
            if column_number in int_columns:
                return True
            else:
                return False
        elif file_name == 'title.csv':
            int_columns = [1, 4]
            if column_number in int_columns:
                return True
            else:
                return False
        else:
            return False