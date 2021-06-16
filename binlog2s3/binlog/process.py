import os
import subprocess


class MySQLBinlogProcess(object):
    def __init__(self, mysqlbinlog_bin, hostname, port, username, password, start_file, tempdir):
        self.mysqlbinlog_bin = mysqlbinlog_bin
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.start_file = start_file
        self.tempdir = tempdir
        self.mysqlbinlog_proc = None
        self.check_tempdir()

    def check_tempdir(self):
        if not os.path.isdir(self.tempdir):
            raise AssertionError("The tempdir {tempdir} is not a directory".format(tempdir=self.tempdir))
        if len(os.listdir(self.tempdir)) != 0:
            raise AssertionError("The {tempdir} is not empty".format(tempdir=self.tempdir))

    @property
    def mysqlbinlog_cmd(self):
        return [self.mysqlbinlog_bin, "--raw", "--read-from-remote-server", "--stop-never",
                "--host", self.hostname, "--port", self.port,
                "-u", self.username, "-p{password}".format(password=self.password), self.start_file]

    def start(self):
        if self.mysqlbinlog_proc:
            raise RuntimeError("A mysqlbinlog process is already running pid {pid}".format(
                pid=self.mysqlbinlog_proc.pid
            ))
        self.mysqlbinlog_proc = subprocess.Popen(
            self.mysqlbinlog_cmd, cwd=self.tempdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
