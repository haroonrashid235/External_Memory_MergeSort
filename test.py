from stream import BufferedInputStream

file_name = 'data/movie_link.csv'
input_stream = BufferedInputStream(file_name)
input_stream.open()

count = 0
while not input_stream.end_of_stream():
	input_stream.read_line()
	count += 1

print(count)

input_stream.close()