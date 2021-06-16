import argparse

from binlog2s3.lib.factory import get_streamer


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--binary", default='/bin/mysqlbinlog', required=False,
                            help="Location of mysqlbinlog binary")
    arg_parser.add_argument("--hostname", required=True,
                            help="Hostname of the mysql server")
    arg_parser.add_argument("--port", required=True,
                            help="Port of the mysql server")
    arg_parser.add_argument("--username", required=True,
                            help="Username for the replication")
    arg_parser.add_argument("--password", required=True,
                            help="Password for the replication")
    arg_parser.add_argument("--start-file", required=True,
                            help="The file to start uploading the binlogs from")
    arg_parser.add_argument("--tempdir", required=True,
                            help="The temporary directory to store the binlogs in, needs to be empty")
    arg_parser.add_argument("--bucket_name", required=True,
                            help="The S3 bucket name to upload the binlogs to")
    args = arg_parser.parse_args()
    streamer = get_streamer(args.binary, args.hostname, args.port, args.username, args.password,
                            args.start_file, args.tempdir, args.bucket_name)
    streamer.run()

if __name__ == "__main__":
    main()
