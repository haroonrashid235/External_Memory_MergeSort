import time
import csv
from external_merge_sort import extsort

file_names = ['complete_cast.csv', 'keyword.csv', 'test_data.csv', 'company_name.csv']
buffer_sizes = [4096, 8192, 16384, 32768, 65536]
merge_streams = [4, 8, 12, 16, 20]

print("EXPERIMENT WITH MULTI-WAY MERGE SORT: ")
for f_name in file_names:
    result_list = []
    header = [f_name, '4K', '8K', '16K', '32K', '64K']
    result_list.append(header)

    for num_stream in merge_streams:
        row = []
        row.append('MS-'+str(num_stream))

        for bsize in buffer_sizes:

            total_time = 0
            for i in range(0, 3):
                ext_sort = extsort(file_name=f_name, k=2, m=bsize, d=num_stream)
                start_time = time.time()
                ext_sort.sort()
                end_time = time.time()
                total_time = total_time+(end_time-start_time)
            avg_time = total_time/3
            row.append(int(avg_time*1000))
            print("File Name: ", f_name, "\t\tBuffer: ", bsize, "\t\tStream: ", num_stream, "\t\tTotal Time: ", int(total_time*1000), "\t\tAverage Time: ", int(avg_time*1000))
        result_list.append(row)

    file_path = 'logs/merge_experiment_'+f_name
    with open(file_path, 'w', newline='') as log_file:
        csv_write = csv.writer(log_file)
        for record in result_list:
            csv_write.writerow(record)
    print("Log Written: ", file_path)