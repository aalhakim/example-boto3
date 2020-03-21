#!python3
"""
Test functions for example-boto3.py.

---
Author: Ali Al-Hakim
Last Updated: 21 March 2020
"""

# Standard library imports
import os
import time
import datetime as dt


#######################################################################
def timeit(func, *args):
    start = time.time()
    r = func(*args)
    end = time.time()
    print(" > {:0<2.4f}s: .{}: {}".format(end-start, func.__name__, r))


def create_local(filepath):
    with open(filepath, "w") as wf:
        wf.write("This is a test file written by:\n {}\n at {}".format(__file__, dt.datetime.now()))


def delete_local(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)


#######################################################################
# TEST FUNCTIONS
class BadlyWrittenTestClass(object):

    def __init__(self, s3_client, local_directory, s3_bucket, s3_directory, test_file):
        
        self.s3_client = s3_client
        self.local_dir = local_directory
        self.s3_bucket = s3_bucket
        self.s3_dir = s3_directory
        self.test_file = test_file
        self.local_filepath = os.path.join(local_directory, test_file).replace("\\", "/")
        self.s3_filepath = os.path.join(s3_bucket, s3_directory, test_file).replace("\\", "/")

    def test_noLocal_noRemote(self):
        print("\nTESTS WHEN FILE DOES NOT EXIST LOCALLY OR REMOTELY")
        delete_local(self.local_filepath)
        self.s3_client.delete_file(self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)

        # Test .upload_file()
        timeit(self.s3_client.upload_file, self.local_dir, self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)    

        # Test ._key_exists(), file does not exist
        timeit(self.s3_client._key_exists, self.s3_dir, self.test_file)

        # Test ._get_object(), file does not exist
        timeit(self.s3_client._get_object, self.s3_dir, self.test_file)

        # Test .get_size(), file does not exist
        timeit(self.s3_client.get_size, self.s3_dir, self.test_file)

        # Test .get_etag(), file does not exist
        timeit(self.s3_client.get_etag, self.s3_dir, self.test_file)

        # Test .download_file(), file exists
        timeit(self.s3_client.download_file, self.s3_dir, self.local_dir, self.test_file)

        # Test .delete_file(), file exists
        timeit(self.s3_client.delete_file, self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)


    def test_yesLocal_noRemote(self):
        print("\nTESTS WHEN FILE EXISTS LOCALLY BUT NOT REMOTELY")
        create_local(self.local_filepath)
        self.s3_client.delete_file(self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)

        # Test ._key_exists(), file does not exist
        timeit(self.s3_client._key_exists, self.s3_dir, self.test_file)

        # Test ._get_object(), file does not exist
        timeit(self.s3_client._get_object, self.s3_dir, self.test_file)

        # Test .get_size(), file does not exist
        timeit(self.s3_client.get_size, self.s3_dir, self.test_file)

        # Test .get_etag(), file does not exist
        timeit(self.s3_client.get_etag, self.s3_dir, self.test_file)

        # Test .download_file(), file does not exist
        timeit(self.s3_client.download_file, self.s3_dir, self.local_dir, self.test_file)

        # Test .delete_file(), file does not exist
        timeit(self.s3_client.delete_file, self.s3_dir, self.test_file)

        # Test .upload_file()
        timeit(self.s3_client.upload_file, self.local_dir, self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)    

        # Test ._key_exists(), file does not exist
        timeit(self.s3_client._key_exists, self.s3_dir, self.test_file)

        # Test ._get_object(), file does not exist
        timeit(self.s3_client._get_object, self.s3_dir, self.test_file)

        # Test .get_size(), file does not exist
        timeit(self.s3_client.get_size, self.s3_dir, self.test_file)

        # Test .get_etag(), file does not exist
        timeit(self.s3_client.get_etag, self.s3_dir, self.test_file)

        # Test .delete_file(), file exists
        timeit(self.s3_client.delete_file, self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)


    def test_noLocal_yesRemote(self):
        print("\nTESTS WHEN FILE EXISTS REMOTELY BUT NOT LOCALLY")
        create_local(self.local_filepath)
        self.s3_client.upload_file(self.local_dir, self.s3_dir, self.test_file)
        delete_local(self.local_filepath)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)

        # Test .upload_file(), no file to upload
        timeit(self.s3_client.upload_file, self.local_dir, self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)    

        # Test ._key_exists(), file exists
        timeit(self.s3_client._key_exists, self.s3_dir, self.test_file)

        # Test ._get_object(), file exists
        timeit(self.s3_client._get_object, self.s3_dir, self.test_file)

        # Test .get_size(), file exists
        timeit(self.s3_client.get_size, self.s3_dir, self.test_file)

        # Test .get_etag(), file exists
        timeit(self.s3_client.get_etag, self.s3_dir, self.test_file)

        # Test .download_file(), create new file locally
        timeit(self.s3_client.download_file, self.s3_dir, self.local_dir, self.test_file)

        # Test .delete_file(), file exists
        timeit(self.s3_client.delete_file, self.s3_dir, self.test_file)

        # Test .get_contents(), file exists
        timeit(self.s3_client.get_contents, self.s3_dir, False)


    def test_yesLocal_yesRemote(self):
        print("\nTESTS WHEN FILE EXISTS LOCALLY AND REMOTELY")
        create_local(self.local_filepath)
        self.s3_client.upload_file(self.local_dir, self.s3_dir, self.test_file)

        # Test .get_contents(), file should exist
        timeit(self.s3_client.get_contents, self.s3_dir, False)

        # Test .upload_file(), file should be overwritten
        timeit(self.s3_client.upload_file, self.local_dir, self.s3_dir, self.test_file)

        # Test ._get_object(), file should exist
        timeit(self.s3_client._get_object, self.s3_dir, self.test_file)

        # Test ._key_exists(), file should exist
        timeit(self.s3_client._key_exists, self.s3_dir, self.test_file)

        # Test .get_size(), file should exist
        timeit(self.s3_client.get_size, self.s3_dir, self.test_file)

        # Test .get_etag(), file should exist
        timeit(self.s3_client.get_etag, self.s3_dir, self.test_file)

        # Test .download_file(), should be successful
        timeit(self.s3_client.download_file, self.s3_dir, self.local_dir, self.test_file)

        # Test .delete_file(), should be successful
        timeit(self.s3_client.delete_file, self.s3_dir, self.test_file)

        # Test .get_contents(), file no longer present
        timeit(self.s3_client.get_contents, self.s3_dir, False)


    def test_objectAttributeDump(self):
        print("\nDUMP AVAILABLE OBJECT ATTRIBUTE DATA")
        create_local(self.local_filepath)
        self.s3_client.upload_file(self.local_dir, self.s3_dir, self.test_file)
        timeit(self.s3_client._key_exists, self.s3_dir, self.test_file)
        timeit(self.s3_client.get_size, self.s3_dir, self.test_file, False)
        timeit(self.s3_client.get_content_type, self.s3_dir, self.test_file, False)
        timeit(self.s3_client.get_etag, self.s3_dir, self.test_file, False)
        timeit(self.s3_client.get_version, self.s3_dir, self.test_file, False)
        timeit(self.s3_client.get_expiration, self.s3_dir, self.test_file, False)
        timeit(self.s3_client.get_expiry_date, self.s3_dir, self.test_file, False)
        timeit(self.s3_client.get_modified_date, self.s3_dir, self.test_file, False)
        delete_local(self.local_filepath)
        self.s3_client.delete_file(self.s3_dir, self.test_file)
