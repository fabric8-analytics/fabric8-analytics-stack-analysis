import json
import os

import boto3
import botocore
import pandas as pd
from util.data_store.abstract_data_store import AbstractDataStore
import numpy as np


class S3DataStore(AbstractDataStore):
    def __init__(self, src_bucket_name, access_key, secret_key):
        self.session = boto3.session.Session(aws_access_key_id=access_key,
                                             aws_secret_access_key=secret_key)
        self.s3_resource = self.session.resource('s3', config=botocore.client.Config(
            signature_version='s3v4'))
        self.bucket = self.s3_resource.Bucket(src_bucket_name)
        self.bucket_name = src_bucket_name

    def get_name(self):
        return "S3:" + self.bucket_name

    def read_json_file(self, filename):
        """Read JSON file from the S3 bucket"""
        return json.loads(self.read_generic_file(filename))

    def read_generic_file(self, filename):
        """Read a file from the S3 bucket."""
        obj = self.s3_resource.Object(self.bucket_name, filename).get()['Body'].read()
        utf_data = obj.decode("utf-8")
        return utf_data

    def list_files(self, prefix=None, max_count=None):
        """List all the files in the S3 bucket"""

        list_filenames = []
        if prefix is None:
            objects = self.bucket.objects.all()
            if max_count is None:
                list_filenames = [x.key for x in objects]
            else:
                counter = 0
                for obj in objects:
                    list_filenames.append(obj.key)
                    counter += 1
                    if counter == max_count:
                        break
        else:
            objects = self.bucket.objects.filter(Prefix=prefix)
            if max_count is None:
                list_filenames = [x.key for x in objects]
            else:
                counter = 0
                for obj in objects:
                    list_filenames.append(obj.key)
                    counter += 1
                    if counter == max_count:
                        break

        return list_filenames

    def read_all_json_files(self):
        """Read all the files from the S3 bucket"""
        list_filenames = self.list_files(prefix=None)
        list_contents = []
        for file_name in list_filenames:
            contents = self.read_json_file(filename=file_name)
            list_contents.append((file_name, contents))
        return list_contents

    def write_json_file(self, filename, contents):
        """Write JSON file into S3 bucket"""
        self.s3_resource.Object(self.bucket_name, filename).put(Body=json.dumps(contents))
        return None

    def upload_file(self, src, target):
        """Upload file into data store"""
        self.bucket.upload_file(src, target)
        return None

    def download_file(self, src, target):
        """Download file from data store"""
        self.bucket.download_file(src, target)
        return None

    def write_pandas_df_into_json_file(self, data, filename):
        self.write_json_file(filename=filename, contents=data.to_json())
        return None

    def read_json_file_into_pandas_df(self, filename):
        json_string = self.read_json_file(filename=filename)
        return pd.read_json(json_string, dtype=np.int8)

    def iterate_bucket_items(self, ecosystem='npm'):
        """
        Generator that iterates over all objects in a given s3 bucket

        See:
        https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2
        for return data format
        :param bucket: name of s3 bucket
        :return: dict of metadata for an object
        """
        client = self.session.client('s3')
        page = client.list_objects_v2(Bucket=self.bucket_name, Prefix=ecosystem)
        yield [obj['Key'] for obj in page['Contents']]
        while page['IsTruncated'] is True:
            page = client.list_objects_v2(Bucket=self.bucket_name, Prefix=ecosystem,
                                          ContinuationToken=page['NextContinuationToken'])
            yield [obj['Key'] for obj in page['Contents']]

    def list_folders(self, prefix=None):
        client = self.session.client('s3')
        result = client.list_objects(Bucket=self.bucket_name, Prefix=prefix + '/', Delimiter='/')
        folders = result.get('CommonPrefixes')
        if not folders:
            return []
        return [folder['Prefix'] for folder in folders]
