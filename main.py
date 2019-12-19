import os

class InputStream:
    def __init__(self, filename):
        self.filename = filename
        self.isOpen = False
        self.file_handler = None

    def open(self):
        if not self.isOpen:
            self.file_handler = open(self.filename, 'r')
            self.isOpen = True 

    def read(self):
        char = self.file_handler.read(1)
        if char == '':
            return False
        return char

    def readline(self):
        if self.isOpen:
            line = []
            char = self.read()
            while char:
                line.append(char)
                char = self.read()
                if char == '\n':
                    line.append(char)
                    break
            return "".join(line)
        else:
            print("Open file handler before reading")

    def seek(self, pos):
        current_pos = self.file_handler.tell()
        return self.file_handler.seek(current_pos + pos)

    def end_of_stream(self):
        if self.read():
            self.seek(-1)
            return False
        return True

        

class OutputStream:
    def  __init__(self):
        pass

    def create(self):
        pass

    def  writeln(self):
        pass

    def close(self):
        pass


# TEST CODE
FILE_NAME = 'imdb/aka_name.csv'
input_stream = InputStream(FILE_NAME)
input_stream.open()
print(input_stream.readline())
print(input_stream.end_of_stream())
print(input_stream.seek(2))
print(input_stream.readline())
print(input_stream.end_of_stream())