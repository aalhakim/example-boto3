#!python3

"""
Example code to upload and download files from AWS S3.

Docs
---
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
"""


# Standard library imports
import os
import logging

# Logger configuration
if __name__ == "__main__":
    import logging.config
    logger_config = "logging_{}.conf".format(os.name)
    logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logger", logger_config))

# Control which modules are allowed to the application log files. This
# will block all logs except Critical logs from the defined group.
for name in ['boto', 'urllib3', 's3transfer', 'boto3', 'botocore', 'nose']:
    logging.getLogger(name).setLevel(logging.CRITICAL)
debugLogger = logging.getLogger(__name__)

# Third-party library imports
from boto3.session import Session
from botocore.exceptions import ClientError


########################################################################
import dotenv  # pip install python-dotenv
dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Collect login details from .env file
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_BUCKET = os.environ["S3_BUCKET"]


########################################################################
class S3Session(object):
    """
    Create an AWS S3 client.
    """

    def __init__(self, bucket_name, access_key, secret_key):
        """ Initialise the S3Session object.
        """
        self.connect(bucket_name, access_key, secret_key)
        self._initialise()

    def connect(self, bucket_name, access_key, secret_key):
        """ Start an S3 session.

        Parameters
        ==========
        bucket_name: <string>
            Name of the bucket to connect to.

        access_key: <string>
            AWS Access Key with permission to access 'bucket_name'.

        secret_key: <string>
            AWS Secret Key with permission to access 'bucket_name'.

        Returns
        =======
        Nothing is returned, but the object variables 'self.s3',
        'self.bucket' and 'self.bucket_name' will be updated.
        """
        session = Session()
        self.s3 = session.resource("s3",aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key)
        self.set_bucket(bucket_name)

    def set_bucket(self, bucket_name):
        """ Set which S3 bucket to access.

        Parameters
        ==========
        bucket_name: <string>
            Name of the bucket to connect to.

        Returns:
        Nothing is returned, but the object variables 'self.bucket' and
        'self.bucket_name' will be updated.
        """
        self.bucket = self.s3.Bucket(bucket_name)
        self.bucket_name = bucket_name

    def get_bucket_name(self):
        """
        Return the name of the active bucket.

        Returns
        =======
        <string>
        Name of currently selected bucket.
        """
        return self.bucket_name

    def _initialise(self):
        """
        Make a dud request to the bucket to initiliase the session.
        """
        self._key_exists("x", "y")

    def get_size(self, s3_directory, filename):
        """
        Return the size in bytes of an S3 Object.

        Description
        ===========
        Retrieve an S3 Object and return the result of the
        content_length method.

        Parameters
        ==========
        s3_directory: <string>
            Filepath to the directory in S3 where 'filename' is found.

        filename: <string>
            Name of the file of interest, including file extension.

        Returns
        =======
        If the file exists: <integer>
            The size of the S3 object in bytes.

        If the file does not exist: <None>
        """
        s3_object = self._get_object(s3_directory, filename)

        if s3_object is not None:
            size_in_bytes = s3_object.content_length
        else:
            size_in_bytes = None

        return size_in_bytes

    def get_etag(self, s3_directory, filename):
        """
        Return the ETag (entity tag) of an S3 Object.

        Description
        ===========
        Retrieve an S3 Object and return the result of the e_tag method.
        The returned result will be an MD5 checksum for all files which
        were uploaded in a single part.

        Parameters
        ==========
        s3_directory: <string>
            Filepath to the directory in S3 where 'filename' is found.

        filename: <string>
            Name of the file of interest, including file extension.

        Returns
        =======
        If the file exists: <string>
            The size of the S3 object in bytes.

        If the file does not exist: <None>
        """
        s3_object = self._get_object(s3_directory, filename)

        if s3_object is not None:
            etag = s3_object.e_tag
        else:
            etag = None

        return etag

    def upload_file(self, src_directory, s3_directory, filename):
        """
        Upload 'src_directory/filename' to 's3_directory/filename'.

        The s3 file will be overwritten if it already exists in the
        destination location ('s3_directory').

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.upload_file

        Parameters
        ==========
        src_directory: <string>
            Filepath to the directory on the local machine where
            'filename' is found.

        s3_directory: <string>
            Filepath to the directory in S3 where 'filename' will be
            uploaded to.

        filename: <string>
            Name of the file of interest, including file extension.

        Returns
        =======
        File exists locally, Upload successful: <boolean> True

        File exists locally, Upload failed: <boolean> False
            # TODO: (AHA) HOW IS THIS SITUATION HANDLED?

        File does not exist locally: <None>
        """
        # Create a filepath from the source directory and filename.
        src_filepath = os.path.join(src_directory, filename).replace("\\", "/")

        # If the file exists in S3
        if os.path.exists(src_filepath) is True:

            # Create a filepath from the destination directory and filename.
            s3_filepath = os.path.join(s3_directory, filename).replace("\\", "/")
            s3_object = self.s3.Object(self.bucket_name, s3_filepath)

            # Upload the target file to S3.
            try:
                s3_object.upload_file(src_filepath)
            except Exception as err:
                debugLogger.error("Upload file failed.", err)
                result = False
            else:
                result = True

        # If the file doesn't exist in S3
        else:
            result = None

        return result

    def download_file(self, s3_directory, dst_directory, filename):
        """
        Download 's3_directory/filename' to 'dst_directory/filename'.

        The local filepath ('dst_directory') will be created if it
        does not already exist. If 'filename' already exists in this
        location it will be overwritten.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.download_file

        Parameters
        ==========
        s3_directory: <string>
            Filepath to the directory in S3 where 'filename' is found.

        dst_directory: <string>
            Filepath to the directory on the local machine where
            'filename' should be saved to.

        filename: <string>
            Name of the file of interest, including file extension.

        Returns
        =======
        File exists in S3, Download successful: <boolean> True
            Filepath where the file was saved on the local machine.

        File exists in S3, Download failed: <boolean> False
            # TODO: (AHA) HOW IS THIS SITUATION HANDLED?

        File does not exist in S3: <None>
        """
        s3_object = self._get_object(s3_directory, filename)

        # If the file exists in S3
        if s3_object is not None:

            # Create a filepath from the source directory and filename.
            dst_filepath = os.path.join(dst_directory, filename).replace("\\", "/")

            # If the file already exists copy the existing file contents
            # for data recovery.
            if os.path.exists(dst_directory) is True:
                with open(dst_filepath, "rb") as rf:
                    backup_data = rf.read()

            # Create the destination filepath if it doesn't exist
            else:
                os.makedirs(dst_directory)

            # Download the target file.
            try:
                s3_object.download_file(dst_filepath)
            except Exception as err:
                result = False
                debugLogger.error("Download file failed.", err)
                with open(dst_filepath, "wb") as wf:
                    wf.write(backup_data)
            else:
                result = True

        # If the file doesn't exist in S3
        else:
            result = None

        return result

    def delete_file(self, s3_directory, filename):
        """
        Delete the file at 's3_directory/filename'.

        This method is currently written to operate with S3 where
        versioning is not enabled. If versioning is enabled delete
        functionality behaves a little differently and this method
        may not return accurate information.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.delete

        Parameters
        ==========
        s3_directory: <string>
            Filepath to the directory in S3 where 'filename' is found.

        filename: <string>
            Name of the file of interest, including file extension.

        Returns
        =======
        File exists in S3, Delete successful: <boolean> True

        File exists in S3, Delete failed: <boolean> False
            # TODO: (AHA) HOW IS THIS SITUATION HANDLED?

        File does not exist in S3: <None>
        """
        s3_object = self._get_object(s3_directory, filename)

        # If the file exists in S3
        if s3_object is not None:

            # Delete the target file.
            try:
                response = s3_object.delete()
            except Exception as err:
                debugLogger.error("Delete file failed.", err)
                result = False
            else:
                result = True

        # If the file doesn't exist in S3
        else:
            result = None

        return result

    def get_contents(self, s3_directory, include_subdirectories=True):
        """
        Retrieve the contents located inside the specified directory.

        Parameters
        ==========
        s3_directory: <string>
            Path to the directory whose contents is of interest. This
            should not include the bucket name.

        include_subdirectories: <boolean>
            Indicate whether files in sub-directories should also be
            returned or not. If set to False then only files immediately
            inside 's3_directory' will be returned.

        Returns
        =======
        <list> of <strings>
        A list of filepaths located inside 'self.bucket_name/s3_directory'
        """
        # Return all contents extending from 's3_directory'
        root = s3_directory.replace("\\", "/")
        contents = [s3_object.key for s3_object in self.bucket.objects.filter(Prefix=root)]

        # Return only contents found immediately in 's3_directory',
        # including folder names.
        if include_subdirectories is False:

            root_items = []
            for filepath in contents:
                root_item = filepath.split("/")[1]
                root_item_filepath = os.path.join(root, root_item).replace("\\", "/")
                if root_item_filepath not in root_items:
                    root_items.append(root_item_filepath)
            contents = root_items

        return contents

    def get_all_contents(self):
        """
        Retrieve all contents inside the bucket.

        Returns
        =======
        <list> of <strings>
        A list of filenames located inside `self.bucket_name`.
        """
        contents = [s3_object.key for s3_object in self.bucket.objects.all()]
        return contents

    def _key_exists(self, s3_directory, filename):
        """
        Check if a key exists in S3.

        Returns
        =======
        """
        exists = self._get_object(s3_directory, filename) is not None
        return exists

    def _get_object(self, s3_directory, filename):
        """
        Retrieve an S3 Object.

        Description
        ===========
        For more information about an S3 Object, see:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object

        Parameters
        ==========
        s3_directory: <string>
            Filepath to the directory in S3 where 'filename' is found.

        filename: <string>
            Name of the file of interest, including file extension.

        Returns
        =======
        If file is found: <boto3.resources.factory.s3.Object>

        If file is not found: <None>
            This is only returned if an 404 error is recieved. Any other
            error will raise an exception.

        Raises
        ======
        If an error other than HTTP 404 is returned, an exception will
        be raised.
        """
        s3_filepath = os.path.join(s3_directory, filename).replace("\\", "/")
        result = self.s3.Object(self.bucket_name, s3_filepath)

        # Check if the object exists in S3.
        try:
            result.load()
        except ClientError as err:
            # ClientError: Object not found
            if err.response["Error"]["Code"] != "404":
                raise
            debugLogger.debug("File not found: {}".format(err))
            result = None

        return result


#######################################################################
def timeit(func, *args):
    start = time.time()
    r = func(*args)
    end = time.time()
    print("{:0<2.4f}s: .{}: {}".format(end-start, func.__name__, r))


########################################################################
if __name__ == "__main__":
    import time
    TEST_FILE = "test.txt"
    LOCAL_DIRECTORY = "./downloads"
    S3_DIRECTORY = "cc-tbnn"

    # Create an Amazon Web Services S3 Client
    s3_client = S3Session(S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY)
    # s3_client.get_content_names(S3_DIRECTORY)

    

    print("TESTS WHEN FILE DOES NOT EXIST\n  ", end="")
    print(os.path.join(S3_BUCKET, S3_DIRECTORY, TEST_FILE))

    # Test ._get_object(), file does not exist
    timeit(s3_client._get_object, S3_DIRECTORY, TEST_FILE)

    # Test ._key_exists(), file does not exist
    timeit(s3_client._key_exists, S3_DIRECTORY, TEST_FILE)

    # Test .get_size(), file does not exist
    timeit(s3_client.get_size, S3_DIRECTORY, TEST_FILE)

    # Test .get_etag(), file does not exist
    timeit(s3_client.get_etag, S3_DIRECTORY, TEST_FILE)

    # Test .download_file(), file does not exist
    timeit(s3_client.download_file, S3_DIRECTORY, LOCAL_DIRECTORY, TEST_FILE)

    # Test .delete_file(), file does not exist
    timeit(s3_client.delete_file, S3_DIRECTORY, TEST_FILE)


    # -----
    print("\nTESTS WHEN FILE EXISTS\n  ", end="")
    print(os.path.join(S3_BUCKET, S3_DIRECTORY, TEST_FILE))

    # Test .upload_file()
    timeit(s3_client.upload_file, LOCAL_DIRECTORY, S3_DIRECTORY, TEST_FILE)

    # Test ._get_object(), file does not exist
    timeit(s3_client._get_object, S3_DIRECTORY, TEST_FILE)

    # Test ._key_exists(), file does not exist
    timeit(s3_client._key_exists, S3_DIRECTORY, TEST_FILE)

    # Test .get_size(), file does not exist
    timeit(s3_client.get_size, S3_DIRECTORY, TEST_FILE)

    # Test .get_etag(), file does not exist
    timeit(s3_client.get_etag, S3_DIRECTORY, TEST_FILE)

    # Test .download_file(), file exists
    timeit(s3_client.download_file, S3_DIRECTORY, LOCAL_DIRECTORY, TEST_FILE)

    # Test .delete_file(), file exists
    timeit(s3_client.delete_file, S3_DIRECTORY, TEST_FILE)

    # Test .get_contents(), file exists
    timeit(s3_client.get_contents, S3_DIRECTORY, False)