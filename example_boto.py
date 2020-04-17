#!python2
#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Example code to upload and download files from AWS S3 using boto.

Compatible with Python 2.7.

---
Authors: Ali Al-Hakim
Last Updated: 21 March 2020
"""

# Standard library imports
import os
import hashlib
import logging
import dotenv  # pip install python-dotenv

# Third-party library imports
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket as S3Bucket
from boto.s3.key import Key as S3Key
from boto.exception import S3ResponseError


#######################################################################
# Setup logging
USE_LOGGER = False
if USE_LOGGER is True:
    import logging
    debugLogger = logging.getLogger(__name__)


#######################################################################
dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_BUCKET = os.environ["S3_BUCKET"]


########################################################################
class AWSS3(object):
    def __init__(self, access_key, secret_key, bucket, verbose=False):
        self.verbose = verbose
        self.connect(access_key, secret_key, bucket)

    ####################################################################
    # CONNECTIONS
    def connect(self, access_key, secret_key, bucket):
        """
        Connect to a specified AWS S3 server and bucket.

        Description
        ===========
        The connection is established and maintained from the following
        attributes generated when this method is called:
            self.conn: <boto.s3.connection.S3Connection>
            self.bucket: <boto.s3.bucket.Bucket>
            self.key: <boto.s3.key.Key>
        See boto API docs:
            https://boto.readthedocs.io/en/latest/ref/s3.html

        Parameters
        ==========
        access_key: <string>
            AWS login credential which can be found through the Amazon
            Management Console with the right permissions.

        secret_key: <string>
            AWS login credential which must be obtained from someone who
            already knows it (like a password). If lost, a new secret
            key must be generated.

        bucket: <string>
            Name of the bucket to access.

        Returns
        =======
        Nothing.

        Raises
        ======
        Nothing.
        """
        if self.verbose: _print("INFO", "Connecting to server")
        try:
            self._set_connection(access_key, secret_key)
        except Exception as e:
            if self.verbose: _print("ERROR", "Could not connect to server.\n\n{}\n".format(e))
            self.connection_established = False
            return

        try:
            self.set_bucket(bucket)
        except Exception as e:
            if self.verbose: _print("ERROR", "Could not find bucket.\n\n{}\n".format(e))
            self.close
            self.connection_established = False
            return

        try:
            self._set_key()
        except Exception as e:
            if self.verbose: _print("ERROR", "Incorrect key. Connection aborted.\n\n{}\n".format(e))
            self.close
            self.connection_established = False
            return

        if self.verbose: _print("INFO", "Successfully connected to bucket '{}'".format(bucket))
        self.connection_established = True

    def _set_connection(self, access_key, secret_key):
        """
        Create a <boto.s3.connection.S3Connection> attribute: self.conn

        Parameters
        ==========
        access_key: <string>
            AWS login credential which can be found through the Amazon
            Management Console with the right permissions.

        secret_key: <string>
            AWS login credential which must be obtained from someone who
            already knows it (like a password). If lost, a new secret
            key must be generated.

        Returns
        =======
        <boolean>
        Indicate if the attribute was successfully created.

        Raises
        ======
        Nothing.
        """
        try:
            self.conn = S3Connection(access_key, secret_key)
        except S3ResponseError as err:
            if self.verbose: _print("ERROR", "{}\n".format(err))
            return False
        return True

    def set_bucket(self, bucket):
        """
        Create a <boto.s3.bucket.Bucket> attribute: self.bucket

        Parameters
        ==========
        bucket: <string>
            Name of the bucket to access.

        Returns
        =======
        <boolean>
        Indicate if the attribute was successfully created.

        Raises
        ======
        Nothing.
        """
        try:
            self.bucket = self.conn.get_bucket(bucket)
        except S3ResponseError as err:
            if self.verbose: _print("ERROR", "{}\n".format(err))
            return False
        return True

    def _set_key(self):
        """
        Create a <boto.s3.key.Key> attribute: self.key

        Returns
        =======
        <boolean>
        Indicate if the attribute was successfully created.

        Raises
        ======
        Nothing.
        """
        try:
            self.key = S3Key(self.bucket)
        except AttributeError as err:
            if self.verbose: _print("ERROR", "{}\n".format(err))
            return False
        return True

    def reconnect(self, access_key, secret_key, bucket):
        """
        Close and reopen a connection to bucket.

        Description
        ===========
        If a connection is already established, it is closed and
        re-established, else it is just established.

        It is advised to reconnect to the server after mutliple requests
        due to request cleanup reasons. I've forgotten the exact details
        but recall reading that these connections should be reset on
        occasion for best performance.

        Parameters
        ==========
        access_key: <string>
            AWS login credential which can be found through the Amazon
            Management Console with the right permissions.

        secret_key: <string>
            AWS login credential which must be obtained from someone who
            already knows it (like a password). If lost, a new secret
            key must be generated.

        bucket: <string>
            Name of the bucket to access.

        Returns
        =======
        Nothing.

        Raises
        ======
        Nothing.

        """
        if self.connection_established:
            self.close()
            self.connect(access_key, secret_key, bucket)
        else:
            self.connect(access_key, secret_key, bucket)

    def close(self):
        """
        Close connections waiting to close.
        """
        if self.verbose: _print("INFO", "Closing open connections")
        self.conn.close()
        self.connection_established = False

    ####################################################################
    # DATA RETRIEVAL
    def get_bucket_name(self):
        """
        Return the name of the active bucket.

        Returns
        =======
        <string>
        Name of currently selected bucket (self.bucket.name).

        Raises
        ======
        Nothing.
        """
        return self.bucket.name

    def get_key(self, keyname, validate=False):
        """
        Return a <boto.s3.key.Key> object with name "keyname".

        Description
        ===========
        Depending on the value of 'validate' this method will either
        retrieve a <boto.s3.key.Key> object through an API request or
        create the object in memory only, which is quicker. Although the
        API request is slower because of a communication delay, it will
        also return metadata (such as if the key exists, size, etag).

        Parameters
        ==========
        keyname: <string>
            Name of the key to be retrieved

        validate=False: <boolean>
            Set True to make an API request, or False to create the key
            object in memory only.

        Returns
        =======
        <boto.s3.key.Key>
        S3 key object with or without metadata (based off validate)

        Raises
        ======
        Nothing.
        """
        # See description above for advice on validate.
        return self.bucket.get_key(keyname, validate=validate)

    def get_all_keys(self):
        """
        Return a list of all keys in the active bucket.

        Returns
        =======
        [list of <boto.s3.key.Key>]
        List of S3 key objects representing all of the keys in the
        currently active bucket (self.bucket).

        Raises
        ======
        Nothing.
        """
        return [key for key in self.bucket.list()]

    def get_keys_from_path(self, path):
        """
        Return a list of keys from a known keypath

        Returns
        =======
        [list of <boto.s3.key.Key>]
        List of S3 key objects representing all of the keys in the
        defined directory of the active bucket (self.bucket).

        Raises
        ======
        Nothing.
        """
        return [key for key in self.bucket.list(prefix=path.replace("\\", "/"))]

    def get_all_keynames(self, prefix=""):
        """
        Return a list of keynames in the bucket.

        Returns
        =======
        [list of <string>]
        List of keynames (excluding the bucket name). These will
        be in the form of filepaths. If the keyname is in the format of
        a filepath, "/" are used to delimit directories.

        Raises
        ======
        Nothing.
        """
        return [os.path.normpath(key.name).replace("\\", "/") for key in self.bucket.list(prefix)]

    def _md5_checksum(self, filepath):
        """
        Complete an md5 checksum on a file.

        Parameters
        ==========
        filepath: <string>
            Absolute file path (i.e. root/dir1/dir2/filename.log) of the
            file to be checksummed.

        Returns
        =======
        <string>
        An md5 checksum of the inserted file.

        Raises
        ======
        Nothing.

        """
        m = hashlib.md5()
        with open(filepath, "rb") as f:
            for block in iter(lambda: f.read(4096), ""):
                m.update(block)
        return m.hexdigest()

    ####################################################################
    # DOWNLOADING
    def download_file(self, src_dir, dst_dir, filename, checksum=True, versioned=False, backup=False):
        """
        Download a file to 'dst_dir' using a filepath.

        The local file ("dst_dir/filename") will be created if

        """
        # Create a filepath from the source directory and filename.
        src_filepath = os.path.join(src_dir, filename).replace("\\", "/")

        # Check if a connection is established.
        if not self.connection_established:
            self._download_message(False, src_filepath)
            return False

        # Download the target file if a connection is established.
        key = self.get_key(src_filepath, validate=True)
        return self._download_from_key(key, dst_dir, checksum, versioned, backup)

    def download_directory(self, src_dir, dst_dir, checksum=True, versioned=False, backup=False):
        """
        Download all files from 'src_dir'
        """
        # Check if a connection is established.
        if not self.connection_established:
            self._download_message(False, src_dir)
            return False

        # Find all keys which have 'src_dir' in their name
        all_keys = self.get_all_keys()
        directory = [key for key in all_keys if key.name.find(src_dir) != -1]

        downloads = 0
        for key in directory:
            src_subdir = os.path.dirname(key.name.replace(src_dir, ""))
            file_dst_dir = os.path.join(dst_dir, src_subdir.strip("/"))
            file_dst_dir = os.path.normpath(file_dst_dir)
            if self._download_from_key(key, file_dst_dir, checksum, versioned, backup):
                downloads += 1

        # Only return true if ALL files were downloaded successfully
        return downloads == len(directory)

    def _download_from_key(self, key, dst_dir, checksum=True, versioned=False, backup=False):
        """
        Download a file to 'dst_dir' using an S3 key.

        Description
        ===========
        If 'checksum' is True, a checksum will be calculated on the
        local and server-side files. If the checksums are equal, the
        download is cancelled. If they are not equal, the local file
        will be updated. If checksum is 'False', the checksum will be
        skipped, but if the file already exists it WILL be overwritten.

        If 'versioned' is True, versioning information will be looked
        up for 'key' and the LATEST version of the file will be
        returned.

        If 'backup' is True, the contents of the original file will be
        saved in memory before being overwritten. Therefore, if there is
        an error (specific error unknown, although some sort of
        connection error) during the overwriting process, the in-memory
        contents will be re-used to ensure no data loss.

        Note: A key with metadata is needed for the checksum
        calculation. Therefore, this function should be faster than
        download() because the key with metadata is already known.

        Parameters
        ==========
        key: <boto.s3.key.Key>
            Boto key object of the file of interest

        dst_dir: <string>
            Destination of the downloaded file. 'dst_dir' should be the
            absolute path (do not include the filename) that the file
            will be downloaded to, such as:
                "C:/root/dir1/dir2/" or "/ubuntu/user/dir1/dir2/"

        checksum=True: <boolean>
            Whether a checksum should be calculated or not. If set to
            False, the key does not need metadata.

        versioned=False: <boolean>
            Whether the file being downloaded is versioned or not.See
            description above for more details.

        backup=False: <boolean>
            Whether the file being overwritten should be backed-up in
            memory before being deleted and re-written with the new
            contents. See description above for more details.

        Returns
        =======
        <boolean>
        Indicates if the download was successful or not (or if the file
        already exists and is up-to-date)

        Raises
        ======
        Nothing.
        """
        # Obtain the source location
        self.key = key
        src_filepath = self.key.name

        # Obtain the destination location
        dst_filepath = os.path.join(dst_dir, os.path.basename(key.name))
        dst_filepath = os.path.normpath(dst_filepath)

        # Check if the key even exists
        if self.key is None:
            if self.verbose: _print("INFO", "'{}' does not exist.".format(src_filepath))
            return False

        # Collect versioning data.
        version = None
        if versioned is True:
            self._get_latest_version(src_filepath)

        # Update 'dst_filepath' if it does not yet exist or if there are
        # differences between the local and online versions, else do
        # nothing. However, if 'checksum' is False, the local file will
        # be overwritten regardless of differences or not.
        result = True
        modified = None
        if os.path.exists(dst_dir):
            if os.path.isfile(dst_filepath):
                # Copy the contents of an existing version of
                # src_filepath if it already exists at the download
                # destination. The get_contents_to_filename(arg)
                # seems to delete the previous version of 'arg'
                # before downloading the new one, hence the
                # requirement to store the data and re-write it if
                # the download fails.
                text = None
                if backup is True:
                    with open(dst_filepath, 'rb') as rf:
                        text = rf.read()

                if checksum is True:
                    md5 = self._md5_checksum(dst_filepath)
                    etag = self.key.etag.strip('"').strip("'")
                    modified = etag != md5
                    if modified:
                        result = self._download(dst_filepath, version, text)

                # If the file exists already, but the checksum is
                # disabled, overwrite the existing file
                else:
                    result = self._download(dst_filepath, version, text)

            # If the file does not exist, download it
            else:
                result = self._download(dst_filepath, version)

        # If the destination directory does not exist, create it and
        # download the file into it.
        else:
            print("DST_DIR='{}'".format(dst_dir))
            os.makedirs(dst_dir)
            result = self._download(dst_filepath, version)

        self._download_message(result, src_filepath, modified)
        return result

    def _get_latest_version(self, filepath):
        """

        """
        version_list = []
        for v in self.bucket.list_versions(prefix=filepath):
            version_list.append(v.version_id)

        # Return the latest version stored.
        if len(version_list) != 0:
            version = version_list[0]

        return version

    def _download(self, dst_filepath, version=None, text=None):
        """
        Download 'dst_filepath' from S3 bucket.
        """
        # try:
        self.key.get_contents_to_filename(dst_filepath, version_id=version)
        # except Exception as e:
        #     if self.verbose: _print("ERROR", "Network is unreachable. Connection lost.\n    1\n{}\n".format(e))
        #     self.connection_established = False
        #     with open(dst_filepath, "w") as wf: wf.write(text)
        #     return False
        return True

    def _download_message(self, successful, filepath, modified=None):
        if self.verbose:
            if successful:
                if modified == True:
                    _print("INFO", "Download: Updated '{}'".format(filepath))
                elif modified == False:
                    _print("INFO", "Download: No change to '{}'".format(filepath))
                else:
                    _print("INFO", "Download: Downloaded '{}'".format(filepath))
            else:
                _print("WARN", "'{}' could not be downloaded.".format(filepath))

    ####################################################################
    # UPLOADING
    def upload(self, src_dir, dst_dir, filename):
        """
        Upload a file to the S3 server

        Parameters
        ==========
        src_dir: <string>
            Location of the local file which is to be uploaded. Should
            be an absolute path, such as:
                "C:/root/dir1/dir2/" or "/ubuntu/user/dir1/dir2/"

        dst_dir: <string>
            Location in S3 where the file should uploaded to. Should be
            an absolute path.

        filename: <string>
            Name of the file being uploaded.

        Returns
        =======
        <boolean>
        Indicate whether the upload was successful or not (of if the
        file alreadyt existed and did not need to be updated).

        Raises
        ======
        Nothing.
        """
        # Create a filepath from the source directory and filename.
        src_filepath = os.path.join(src_dir, filename).replace("\\", "/")

         # Check if a connection is established.
        if not self.connection_established:
            self._upload_message(False, src_filepath)
            return False

        dst_filepath = os.path.join(dst_dir, filename).replace("\\", "/")
        self.key = self.get_key(dst_filepath, validate=True)

        # Update 'dst_filepath' if it does not yet exist or if there are
        # differences between the local and online versions, else do
        # nothing.
        result = True
        modified = None
        if self.key is None:
            self.key = self.get_key(dst_filepath)
            result = self._upload(src_filepath)
        else:
            md5 = self._md5_checksum(src_filepath)
            etag = self.key.etag.strip('"').strip("'")
            modified = etag != md5
            if modified:
                result = self._upload(src_filepath)

        self._upload_message(result, dst_filepath, modified)
        return result

    def _upload(self, src_filepath):
        # try:
        self.key.set_contents_from_filename(src_filepath)
        # except Exception as e:
        #     if self.verbose: _print("ERROR", "Network is unreachable. Connection lost.\n\n{}\n".format(e))
        #     self.connection_established = False
        #     return False
        return True

    def _upload_message(self, successful, filepath, modified):
        if self.verbose:
            if successful:
                if modified is True:
                    _print("INFO", "Upload: Updated '{}'".format(filepath))
                elif modified is False:
                    _print("INFO", "Upload: No change to '{}'".format(filepath))
                else:
                    _print("INFO", "Upload: Uploaded '{}'".format(filepath))
            else:
                _print("WARN", "'{}' was not uploaded.".format(filepath))

    ####################################################################
    # DELETING
    def delete(self, dirname, filename):
        # Create a filepath from the source directory and filename.
        filepath = os.path.join(dirname, filename).replace("\\", "/")

        # Check if a connection is established.
        if not self.connection_established:
            self._delete_message(False, filepath)
            return False

        # Delete the key online file if it exists.
        result = None
        self.key = self.get_key(filepath, validate=True)
        if self.key is not None:
            result = self._delete()

        self._delete_message(result, filepath)
        return result

    def _delete(self):
        # try:
        self.key.delete()
        # except Exception as e:
        #     if self.verbose: _print("ERROR", "Network is unreachable. Connection lost.\n\n{}\n".format(e))
        #     self.connection_established = False
        #     return False
        return True

    def _delete_message(self, successful, filepath):
        if self.verbose:
            if successful is None:
                _print("WARN",
                    "'{}' could not be deleted because it does not exist.".format(filepath)
                    )
            elif successful:
                _print("INFO", "Deleted '{}'.".format(filepath))
            else:
                _print("WARN", "'{}' could not be deleted.".format(filepath))


########################################################################
def _print(level, string):
    if USE_LOGGER is True:
        if level == "DEBUG":
            debugLogger.debug(string)
        elif level == "INFO":
            debugLogger.info(string)
        elif level == "WARN":
            debugLogger.warn(string)
        elif level == "ERROR":
            debugLogger.error(string)
        else:
            debugLogger.debug("UNKNOWN LEVEL: " + string)
    else:
        print("{}: {}".format(level, string))


#######################################################################
def posix_filepath(*args):
    """
    Return a normalised filepath format.

    Description
    ===========
    Create a posix format filepath. All backslahes are replaced with
    forward slashes which is the format used by AWS S3 keys.

    Parameters
    ==========
    args: series of <strings>
        Folder and file names in the order they should be concatenated.

    Returns
    =======
    <string>: a filepath with only '/' for separators.

    """
    path = os.path.join(*args)

    return path.replace("\\", "/")


import datetime as dt
def create_local(filepath):
    if not os.path.exists(filepath):
        os.makedirs(os.path.dirname(filepath))

    with open(filepath, "w") as wf:
        wf.write("This is a test file written by:\n {}\n at {}".format(__file__, dt.datetime.now()))


def delete_local(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)



########################################################################
if __name__ == "__main__":

    s3_client = AWSS3(S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, verbose=True)

    TEST_FILE = "test.txt"
    LOCAL_DIRECTORY = "./s3files"
    S3_DIRECTORY = "test-dir"
    LOCAL_FILEPATH = posix_filepath(LOCAL_DIRECTORY, TEST_FILE)
    S3_FILEPATH = posix_filepath(S3_BUCKET, S3_DIRECTORY, TEST_FILE)

    print("\n Local: {}".format( LOCAL_FILEPATH))
    print("Remote: {}\n".format(S3_FILEPATH))


    create_local(LOCAL_FILEPATH)                                        # Create local
    s3_client.upload(LOCAL_DIRECTORY, S3_DIRECTORY, TEST_FILE)          # Upload
    s3_client.download_file(S3_DIRECTORY, LOCAL_DIRECTORY, TEST_FILE)   # Download
    s3_client.download_file(S3_DIRECTORY, LOCAL_DIRECTORY, TEST_FILE)   # Overwrite local
    s3_client.upload(LOCAL_DIRECTORY, S3_DIRECTORY, TEST_FILE)          # Overwrite remote

    with open(LOCAL_FILEPATH, "a") as wf:                               # Modify local
        wf.write("Hello, my name is Waldo. Or is it Wally?")

    s3_client.upload(LOCAL_DIRECTORY, S3_DIRECTORY, TEST_FILE)          # Upload modified
    s3_client.download_file(S3_DIRECTORY, LOCAL_DIRECTORY, TEST_FILE)   # Download modified
    s3_client.delete(S3_DIRECTORY, TEST_FILE)                           # Delete remote
    delete_local(LOCAL_FILEPATH)                                        # Delete local

    print("complete\n")
