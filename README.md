Warning
=======

This repo is only a proof of concept. Do not use it for production.

Installation
============

This package can be installed with easy install, I recommand installing it in a clean virtualenv.
```
python setup.py install
```

Usage
=====

The binlog2s3 utility needs:
- a mysqlbinlog binary
- an empty local directory (binlogs will be streamed here)
- an empty S3 bucket (binlogs will be uploaded here)

Once this is done, it can be started with the following command.
```
$ binlog2s3 \
  --binary /usr/local/bin/mysqlbinlog \
  --hostname exmapledb1.10.0.0.1.nip.io \
  --port 3306 \
  --username repl \
  --password passwordhere \
  --start-file mysql-bin.000001 \
  --tempdir ${HOME}/tmpdir \
  --bucket_name BUCKET_NAME_HERE
```

Once the process is started, it starts a mysqlbinlog process which streams the binary logs to the local directory 
from there, they are streamed to S3 with multipart uploads (5M a time). If there are no parts to upload, the program
will wait for more binary logs to arrive. 

The temp directory needs to be empty when this is started. 
