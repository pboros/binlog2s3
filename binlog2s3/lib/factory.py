def get_binlog_process(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir):
    from binlog2s3.binlog.process import MySQLBinlogProcess
    return MySQLBinlogProcess(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir)


def get_binlog_reader(tempdir):
    from binlog2s3.binlog.reader import MySQLBinlogReader
    return MySQLBinlogReader(tempdir)


def get_s3_uploader(bucket_name, filename):
    from binlog2s3.s3.uploader import S3Uploader
    return S3Uploader(bucket_name, filename)

def get_streamer(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir, bucket_name):
    from binlog2s3.stream.stream import StreamBinlogs
    return StreamBinlogs(mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir, bucket_name)
