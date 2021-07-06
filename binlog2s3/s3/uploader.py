import datetime
import boto3
import os


from botocore.client import ClientError


class S3Uploader(object):
    def __init__(self, bucket_name, filename):
        self.bucket_name = bucket_name
        self.session = boto3.Session()
        self.multipart_key = filename
        self.s3_client = self.session.client("s3")
        self.part_number = 0
        self.multipart_upload = None
        self.part_info = {
            'Parts': []
        }

    def test_bucket_access(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            raise AssertionError("Could not access S3 bucket {name}".format(name=self.bucket_name))

    def create_multipart_upload(self):
        print("{dt} Creating multipart uploader for {filename}".format(
            dt=datetime.datetime.now(), filename=self.multipart_key
        ))
        self.multipart_upload = self.s3_client.create_multipart_upload(Bucket=self.bucket_name, Key=self.multipart_key)

    def upload_part(self, data):
        self.part_number += 1
        print("{dt} Uploading part {number} for {filename} size {size}".format(
            dt=datetime.datetime.now(), number=self.part_number, filename=self.multipart_key, size=len(data)
        ))
        part = self.s3_client.upload_part(
            Bucket=self.bucket_name, Key=self.multipart_key, PartNumber=self.part_number,
            UploadId=self.multipart_upload['UploadId'], Body=data
        )
        self.part_info['Parts'].append({
            'PartNumber': self.part_number,
            'ETag': part['ETag']
        })

    def close_multipart_upload(self):
        print("{dt} Finishing multipart upload for {filename}".format(
            dt=datetime.datetime.now(), filename=self.multipart_key
        ))
        self.s3_client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=self.multipart_key, UploadId=self.multipart_upload['UploadId'],
            MultipartUpload=self.part_info
        )
