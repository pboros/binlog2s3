import io
import time


from binlog2s3.lib.factory import get_binlog_process, get_binlog_reader, get_s3_uploader


class StreamBinlogs(object):
    MIN_PART_SIZE = 5 * 1024 * 1024 # 5M
    DATA_WAIT_SPIN_INTERVAL = 0.3

    def __init__(self, mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir, bucket_name):
        self.mysqlbinlog_bin = mysqlbinlog_bin
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.start_file = start_file
        self.tempdir = tempdir
        self.bucket_name = bucket_name
        self.binlog_process = get_binlog_process(
            self.mysqlbinlog_bin, self.hostname, self.port, self.username, self.password, self.start_file, self.tempdir
        )
        self.binlog_reader = get_binlog_reader(self.tempdir)
        self.s3_uploader = None
        self.current_file = None
        self.buf = io.BytesIO()

    def open_new_file(self):
        self.current_file = self.binlog_reader.current_file
        self.s3_uploader = get_s3_uploader(self.bucket_name, self.current_file)
        self.s3_uploader.create_multipart_upload()

    def rotate_file(self):
        self.s3_uploader.close_multipart_upload()
        self.open_new_file()

    def reset_buf(self):
        self.buf.truncate(0)
        self.buf.close()
        self.buf = io.BytesIO()

    def run(self):
        self.binlog_process.start()
        while True:
            chunk = self.binlog_reader.get_next_chunk()
            if self.current_file is None:
                # This is the first chunk of the file, create new one and multipart uploader for it
                self.open_new_file()
            if self.binlog_reader.current_file != self.current_file:
                # File rotation happened, the last part can be less than the minimum part size
                self.s3_uploader.upload_part(self.buf.getvalue())
                self.reset_buf()
                # The filename changed since last chunk, need to close the old one and open a new one
                self.rotate_file()
                self.buf.write(chunk)
            if len(self.buf.getvalue()) < self.MIN_PART_SIZE:
                # Not enough data in the buffer to create a part
                self.buf.write(chunk)
                # Avoid burning the CPU when there is no or little data to read
                time.sleep(self.DATA_WAIT_SPIN_INTERVAL)
            else:
                # Enough data in the buffer
                # Upload the data as a part
                self.s3_uploader.upload_part(self.buf.getvalue())
                # Reset the buffer
                self.reset_buf()
                self.buf.write(chunk)
