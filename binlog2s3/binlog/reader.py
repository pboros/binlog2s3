import os
import time


class MySQLBinlogReader(object):
    CHUNK_SIZE = 5 * 1024 * 1024 # 5M

    def __init__(self, tempdir):
        self.tempdir = tempdir
        self.current_file = None
        self.current_fullpath = None
        self.file_in_progress = False
        self.fd = None

    def get_file_list(self):
        return sorted(os.listdir(self.tempdir))

    def is_last_in_tempdir(self, filename):
        file_list = self.get_file_list()
        if filename == file_list[-1]:
            return True
        else:
            return False

    def get_current_file(self):
        while not self.current_file:
            file_list = self.get_file_list()
            if file_list:
                self.current_file = file_list[0]
                self.current_fullpath = "{dirname}/{filename}".format(dirname=self.tempdir, filename=self.current_file)
                self.fd = open(self.current_fullpath, 'rb')
                self.file_in_progress = True
            else:
                print("Waiting for binlog files to appear")
                time.sleep(1)

    def close_current_file(self):
        self.fd.close()
        self.current_file = None
        self.current_fullpath = None
        self.fd = None

    def get_next_chunk(self):
        self.get_current_file()
        chunk = self.fd.read(self.CHUNK_SIZE)
        # This is not the last file in the temp directory, we might hit the end
        if not self.is_last_in_tempdir(self.current_file):
            # Didn't read any new data
            if len(chunk) == 0:
                # The pointer is at the end of the file
                if os.path.getsize(self.current_fullpath) == self.fd.tell():
                    fullpath = self.current_fullpath
                    self.close_current_file()
                    os.unlink(fullpath)
                    self.get_current_file()
        return chunk





