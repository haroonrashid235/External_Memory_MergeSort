__author__ = "Haroon Rashid"
__email__ = "haroon.rashid235@gmail.com"


import os
import mmap
import sys

class ByteInputStream:
    def __init__(self, filename):
        """ 
        Creates the ByteInputStream Object to read file a byte at a time.
        
        Parameters:
            filename (string): path to the file to read
        
        Returns:
            ByteInputStream Object   
        """
        self.filename = filename
        self.is_open = False
        self.file_handler = None

    def open(self):
        """ 
        Creates the File Handler with buffering=0 for reading file a byte at a time.

        Returns:
            self.file_handler (_io.FileIO): IO File Handler object returned by the open fuction.   
        """
        # Do not open already opened file
        if not self.is_open:
            self.file_handler = open(self.filename, 'rb', buffering=0)
            self.is_open = True
            return self.file_handler


    def read_byte(self):
        """ 
        Reads and returns a single byte from a self.filename file at the current seek position.
        
        Returns:
            raw_byte (bytes): Byte read from the file, return False if no more byte is available.   
        """
        # read(n) reads n byte, reading 1 byte here
        raw_byte = self.file_handler.read(1)
        if raw_byte == b'':
            return False
        return raw_byte


    def read_line(self):
        """ 
        Reads and returns a line from a self.filename file strating at the current seek position.
        
        Returns:
            line (str): Line of chars read from the file as string.   
        """
        # read(n) reads n byte, reading 1 byte here
        if self.is_open:
            line = []
            raw_byte = self.read_byte()
            while raw_byte:
                # try:
                #     # decode bytes to string using utf-8 decoding
                #     char = raw_byte
                # # Check for UnicodeDecode Exceptions
                # except UnicodeDecodeError:
                #     raw_byte = self.read_byte()
                #     continue
                line.append(raw_byte)
                raw_byte = self.read_byte()

                # Line reading is completed at this new-line delimeter
                if raw_byte == b'\n':
                    line.append(raw_byte)
                    break
            # print(b"".join(line), line[0])
            return b''.join(line)
        else:
            raise ValueError("I/O operation on closed file.")


    def seek(self, pos, absolute = True):
        """ 
        Seeks or moves the file reading pointer to a pos postion in the file.
        
        Parameters:
            pos (int): position to seek to, specified as integer
            absolute (bool): False moves the pointer pos steps from the current position,
                             True moves the pointer to the absolute pos position.
        Returns:
            seek_pos (int): Current seek position after moving.   
        """
        if not absolute:
            current_pos = self.file_handler.tell()
            seek_pos = current_pos + pos
        else:
            seek_pos = pos
        return self.file_handler.seek(seek_pos)


    def end_of_stream(self):
        """ 
        Returns a boolean to indicate the end of stream.
        
        Returns:
            boolean (bool): Boolean to indicate the end of file stream.   
        """
        if self.read_byte():
            self.file_handler.seek(-1, 1)
            return False
        return True


    def close(self):
        """ 
        Close the filestream object, raises exception if file is already closed.   
        """
        if self.is_open:
            self.is_open = False
            self.file_handler.close()
        else:
            raise Exception("Cannot close a closed File")




class BufferedInputStream:
    
    def __init__(self, filename, buffer_size=None):
        """ 
        Creates the BufferedInputStream Object to read file using main memory buffers.
        
        Parameters:
            filename (string): path to the file to read
            buffer_size (int, None): If None, use default buffer. If int, use the buffer_size as buffer. 
        
        Returns:
            BufferedInputStream Object   
        """
        self.filename = filename
        self.is_open = False
        self.file_handler = None
        self.buffer_size = buffer_size
        self.buffer = None  # Stores the iterator over the buffer contents 


    def open(self):
        """ 
        Creates the File Handler with buffering=buffer_size for reading file.
        
        Returns:
            self.file_handler (_io.FileIO): IO File Handler object returned by the open fuction.   
        """
        if not self.is_open:
            # If buffer_size is not specified, use default buffering mechanism
            if self.buffer_size is None:
                self.file_handler = open(self.filename,'r')
            else:
                # Use buffering specified by the buffer_size 
                self.file_handler = open(self.filename,'r', buffering = self.buffer_size)
            self.is_open = True
            # Get the reference to the buffer
            self.buffer = self.file_handler.buffer
            return self.file_handler


    def read_lines(self):
        """ 
        Reads and yields a line from a self.filename file strating at the current seek position.
        yield allows to use the function as an iterator.
        
        Returns:
            line (str): Yields Line of chars read from the file as string.   
        """
        # read(n) reads n byte, reading 1 byte here
        for line in self.buffer:
            yield line
        # return next(self.buffer)
        # return self.file_handler.readline()  

    def read_line(self):
        try:
            line = self.file_handler.readline()
        except:
            line = ''
        return line
        # return next(self.buffer)#.decode('utf-8')


    def seek(self, pos, absolute = True):
        """ 
        Seeks or moves the file reading pointer to a pos postion in the file.
        
        Parameters:
            pos (int): position to seek to, specified as integer
            absolute (bool): False moves the pointer pos steps from the current position,
                             True moves the pointer to the absolute pos position.
        Returns:
            seek_pos (int): Current seek position after moving.   
        """
        if not absolute:
            current_pos = self.file_handler.tell()
            seek_pos = current_pos + pos
        else:
            seek_pos = pos
        return self.file_handler.seek(seek_pos)


    def end_of_stream(self):
        """ 
        Returns a boolean to indicate the end of stream.
        
        Returns:
            boolean (bool): Boolean to indicate the end of file stream.   
        """
        return self.buffer.peek() == b''
        # if self.read_byte():
        #     self.seek(-1, False)
        #     return False
        # return True


    def close(self):
        """ 
        Close the filestream object, raises exception if file is already closed.   
        """
        if self.is_open:
            self.is_open = False
            self.file_handler.close()
        else:
            raise Exception("Cannot close a closed File")


class MemMappedInputStream:
    
    def __init__(self, filename, buffer_size):
        """ 
        Creates the BufferedInputStream Object to read file using main memory buffers.
        
        Parameters:
            filename (string): path to the file to read
            buffer_size (int, None): If None, use default buffer. If int, use the buffer_size as buffer. 
        
        Returns:
            BufferedInputStream Object   
        """
        assert buffer_size % mmap.ALLOCATIONGRANULARITY == 0
        self.filename = filename
        self.is_open = False
        self.file_handler = None
        self.buffer_size = buffer_size
        self.buffer = None
        self.map_file = None
        self.offset = 0
        self.step_size = mmap.ALLOCATIONGRANULARITY
        self.position = 0
        self.file_size = os.path.getsize(self.filename)
        self.remaining = self.file_size
        self.eof = False
        self.temp_line = None

    def open(self):
        """ 
        Creates the File Handler with buffering=buffer_size for reading file.
        
        Returns:
            self.file_handler (_io.FileIO): IO File Handler object returned by the open fuction.   
        """
        if not self.is_open:
            # If buffer_size is not specified, use default buffering mechanism
            self.file_handler = open(self.filename,'r+')
            self.is_open = True
            self.buffer = self.file_handler.buffer
            self.allocate_memory()
            return self.file_handler

    def allocate_memory(self):
        if self.remaining > 0:
            if self.map_file is not None:
                self.map_file.flush()
            
            # If reading the last portion of the file, map only the left-over part of the filename
            if self.file_size - self.offset < self.buffer_size:
                self.buffer_size = self.file_size - self.offset
            
            self.map_file = mmap.mmap(self.file_handler.fileno(), self.buffer_size, access=mmap.ACCESS_READ, offset=self.offset)
            self.remaining -= self.buffer_size
            self.position = 0
            self.offset += self.step_size
            return True
        return False

    def read_line(self):
        if self.position >= self.buffer_size:
            if self.allocate_memory():    
                line = self.map_file.readline()
                if self.temp_line is not None:
                    line = self.temp_line + line
                    self.temp_line = None 
                self.position += len(line)
            else:
                self.eof = True
                line = ''
        else:
            line = self.map_file.readline()
            self.position += len(line)
            if line != b'' and line[-1] != 10:
                self.temp_line = line
                line = b''
        return line

    def seek(self, pos):
        """ 
        Seeks or moves the file reading pointer to a pos postion in the file.
        
        Parameters:
            pos (int): position to seek to, specified as integer
            absolute (bool): False moves the pointer pos steps from the current position,
                             True moves the pointer to the absolute pos position.
        Returns:
            seek_pos (int): Current seek position after moving.   
        """
        # If the seek position is beyond the currently mapped part of file
        multiplier = int(pos / self.step_size)
        self.offset = self.step_size * multiplier
        self.allocate_memory()
        seek_pos = abs(pos - (self.offset - self.step_size))
        try:
            self.map_file.seek(seek_pos)
        except:
            return


    def end_of_stream(self):
        """ 
        Returns a boolean to indicate the end of stream.
        
        Returns:
            boolean (bool): Boolean to indicate the end of file stream.   
        """
        return self.eof
        # if self.read_byte():
        #     self.seek(-1, False)
        #     return False
        # return True


    def close(self):
        """ 
        Close the filestream object, raises exception if file is already closed.   
        """
        if self.is_open:
            self.is_open = False
            self.map_file.flush()
            self.map_file.close()
            self.file_handler.close()
        else:
            raise Exception("Cannot close a closed File")

class OutputStream:
    def  __init__(self,filename):
        self.filename = filename
        self.file_handler = None

    def create(self):
        if os.path.exists(self.filename):
            self.file_handler = open(self.filename, 'a')
        else:
            self.file_handler = open(self.filename, 'w')
        return self.file_handler

    def write_line(self, string):
        for char in string:
            self.file_handler.write(char)
        return self.file_handler.write('\n')

    def close(self):
        """ 
        Close the filestream object, raises exception if file is already closed.   
        """
        self.file_handler.close()

class BufferedOutputStream:
    def  __init__(self, filename, buffer_size = None):
        self.filename = filename
        self.file_handler = None
        self.buffer_size = buffer_size

    def create(self):
        if os.path.exists(self.filename):
            if self.buffer_size is None:
                self.file_handler = open(self.filename, 'a')
            else:
                self.file_handler = open(self.filename, 'a', buffering = self.buffer_size)
        else:
            if self.buffer_size is None:
                self.file_handler = open(self.filename, 'w')
            else:
                self.file_handler = open(self.filename, 'w', buffering = self.buffer_size)
        return self.file_handler

    def write_line(self, string):
        return self.file_handler.write(string)

    def close(self):
        """ 
        Close the filestream object, raises exception if file is already closed.   
        """
        self.file_handler.close()


class MemMappedOutputStream:
    def  __init__(self, filename, buffer_size):
        assert buffer_size % mmap.ALLOCATIONGRANULARITY == 0
        self.filename = filename
        self.file_handler = None
        self.buffer_size = buffer_size
        self.map_file = None
        self.position = 0
        self.step_size = mmap.ALLOCATIONGRANULARITY
        self.offset = 0
        
    def create(self):
        self.file_handler = open(self.filename,'w')
        self.allocate_memory()
        return self.file_handler

    def allocate_memory(self):
        if self.map_file is not None:
            self.map_file.flush()
        self.map_file = mmap.mmap(self.file_handler.fileno(), self.buffer_size, access=mmap.ACCESS_WRITE, offset=self.offset)
        self.position = 0
        self.offset += self.step_size

    def write_line(self, string):
        if self.position >= self.buffer_size:
            self.allocate_memory()
        self.map_file.flush()
        self.map_file[self.position:self.position + len(string)] = string
        return True

    def close(self):
        """ 
        Close the filestream object, raises exception if file is already closed.   
        """
        self.map_file.flush()
        self.map_file.close()
        self.file_handler.close()
        
