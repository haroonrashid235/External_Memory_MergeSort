class ByteInputStream:
    def __init__(self, filename):
        self.filename = filename
        self.is_open = False
        self.file_handler = None

    def open(self):
        if not self.is_open:
            self.file_handler = open(self.filename, 'rb', buffering=0)
            self.is_open = True
            return self.file_handler


    def read_byte(self):
        raw_byte = self.file_handler.read(1)
        if raw_byte == b'':
            return False
        return raw_byte


    def read_line(self):
        if self.is_open:
            line = []
            raw_byte = self.read_byte()
            while raw_byte:
                try:
                    char = raw_byte.decode('utf-8')
                except UnicodeDecodeError:
                    raw_byte = self.read_byte()
                    continue
                line.append(char)
                raw_byte = self.read_byte()
                if raw_byte == b'\n':
                    break
            return "".join(line)
        else:
            raise ValueError("I/O operation on closed file.")


    def seek(self, pos, absolute = True):
        if not absolute:
            current_pos = self.file_handler.tell()
            seek_pos = current_pos + pos
        else:
            seek_pos = pos
        return self.file_handler.seek(seek_pos)


    def end_of_stream(self):
        if self.read_byte():
            self.file_handler.seek(-1, 1)
            return False
        return True


    def close(self):
        if self.is_open:
            self.is_open = False
            self.file_handler.close()
        else:
            raise Exception("Cannot close a closed File")




class BufferedInputStream:

    def __init__(self, filename, buffer_size=None):
        self.filename = filename
        self.is_open = False
        self.file_handler = None
        self.buffer_size = buffer_size
        self.buffer = None


    def open(self):
        if not self.is_open:
            if self.buffer_size is None:
                self.file_handler = open(self.filename,'r')
            else:
                self.file_handler = open(self.filename,'r', buffering = self.buffer_size)
            self.is_open = True
            self.buffer = self.file_handler.buffer
            return self.file_handler


    def read_byte(self):
        raw_byte = self.file_handler.read(1)
        if raw_byte == b'':
            return False
        return raw_byte


    def read_lines(self):
        for line in self.buffer:
            yield line
        # return next(self.buffer)
        # return self.file_handler.readline()    


    def seek(self, pos, absolute = True):
        if not absolute:
            current_pos = self.file_handler.tell()
            seek_pos = current_pos + pos
        else:
            seek_pos = pos
        return self.file_handler.seek(seek_pos)


    def end_of_stream(self):
        return self.buffer.peek() == b''
        # if self.read_byte():
        #     self.seek(-1, False)
        #     return False
        # return True


    def close(self):
        if self.is_open:
            self.is_open = False
            self.file_handler.close()
        else:
            raise Exception("Cannot close a closed File")


class OutputStream:
    def  __init__(self):
        pass

    def create(self):
        pass

    def  writeln(self):
        pass

    def close(self):
        pass