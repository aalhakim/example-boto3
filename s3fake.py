"""
Code to replace the actual AWSS3 class when there is no internet connection
to enable testing without internet. Documents will be written to and read
from a Windows file location instead.

AWSS3 functions mimicked:
    connect
    download_file
    upload

"""

########################################################################
USE_LOGGER = False
if USE_LOGGER is True:
    import logging
    debugLogger = logging.getLogger(__name__)


########################################################################
import os
from shutil import copyfile
from awss3 import AWSS3


########################################################################
S3FAKE_DIRECTORY = os.path.join(os.path.dirname(__file__), "../")


########################################################################
class AWSS3Fake(AWSS3):
    def __init__(self, access_key, secret_key, bucket, verbose=False):
        """
        Initialise the object.

        Creates
        =======
        self.conn: <None>
        self.key: <None>
            These variables are not required for AWSS3Fake

        self.bucket: <str>
            Stores the buck name as a string. This is treated as the
            top-level directory in the local file store.
        """
        self.conn = None
        self.key = None
        self.bucket = bucket
        self.basedir = os.path.join(S3FAKE_DIRECTORY, bucket)
        self.verbose = verbose
        self.connect(access_key, secret_key, bucket)

    ####################################################################
    # CONNECTIONS
    def connect(self, access_key, secret_key, bucket):
        """
        This method simply checks of the local file store directory
        exists or not. If it doesn't it will create it.

        Creates
        =======
        self.connection_established: <bool>
            Used to indicate if a connection is established or not.
            (This variable is not useful for AWSS3Fake)

        Returns
        =======
        Nothing.

        Raises
        ======
        Nothing.
        """
        if self.verbose:
            _print("INFO", "Creating local file store in {}".format(self.basedir))

        # Create the base directory if it doesn't already exist
        create_directory(self.basedir)

        self.connection_established = True

    def set_bucket(self, bucket):
        """
        Update the `self.bucket`  to `bucket`.

        Parameters
        ==========
        bucket: <str>
            The name of the new top-level directoy files should be
            copied into.

        """
        self.bucket = bucket
        self.basedir = os.path.join(S3FAKE_DIRECTORY, bucket)
        create_directory(self.basedir)

    def reconnect(self, access_key, secret_key, bucket):
        """
        This method does nothing for AWSS3Fake.
        """
        pass

    def close(self):
        """
        This method will set `self.connection_established to False which
        is expected but otherwise does nothing.
        """
        if self.verbose: _print("INFO", "Closing open connections")
        self.connection_established = False

    ####################################################################
    # DOWNLOADING
    def download_file(self, src_dir, dst_dir, filename, checksum=True, versioned=False, backup=False):
        """
        Download a file to 'dst_dir' using a filepath.

        The local file ("dst_dir/filename") will be created if it
        doesn't already exist.

        Returns
        =======
        <boolean>
        Indicates if the download was successful or not (or if the file
        already exists and is up-to-date)

        Raises
        ======
        Nothing.

        """
        # Create a filepath from the source directory and filename.
        src_filepath = os.path.join(self.basedir, src_dir, filename).replace("\\", "/")

        # Check if the file exists at the destination
        if not os.path.exists(src_filepath):
            if self.verbose: _print("INFO", "'{}' does not exist.".format(src_filepath))
            return False

        # Update 'dst_filepath' if it does not yet exist or if there are
        # differences between the local and online versions, else do
        # nothing. However, if 'checksum' is False, the local file will
        # be overwritten regardless of differences or not.
        dst_filepath = os.path.join(self.basedir, filename)
        result = True
        modified = None

        # Check if the file already exists
        if os.path.exists(dst_dir):
            if os.path.isfile(dst_filepath):

                # Overwrite the existing file if it is out of date
                if checksum is True:
                    md5_dst = self._md5_checksum(dst_filepath)
                    md5_src = self._md5_checksum(src_filepath)
                    modified = md5_src != md5_dst
                    if modified:
                        result = copy_file(src_filepath, dst_filepath)

                # If the file exists already, but the checksum is
                # disabled, overwrite the existing file
                else:
                    result = copy_file(src_filepath, dst_filepath)

            # If the file does not exist, download it
            else:
                result = copy_file(src_filepath, dst_filepath)

        # If the destination directory does not exist, create it and
        # download the file into it.
        else:
            os.makedirs(dst_dir)
            result = copy_file(src_filepath, dst_filepath)

        self._download_message(result, src_filepath, modified)
        return result

    ####################################################################
    # UPLOADING
    def upload(self, src_dir, dst_dir, filename):
        """
        Upload file in `src_dir` to `dst_dir`

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
        # Create a filepath for the source and destination files.
        src_filepath = os.path.join(src_dir, filename).replace("\\", "/")
        dst_filepath = os.path.join(self.basedir, dst_dir, filename).replace("\\", "/")

        # Update 'dst_filepath' if it does not yet exist or if there are
        # differences between the local and online versions, else do
        # nothing.
        result = True
        modified = None

        # If the file doesn't exist in the 'server' location, then
        # upload it
        if not os.path.exists(dst_filepath):
            result = copy_file(src_filepath, dst_filepath)

        # Overwrite the 'server' file if it is different, otherwise
        # do nothing.
        else:
            md5_src = self._md5_checksum(src_filepath)
            md5_dst = self._md5_checksum(dst_filepath)
            modified = md5_dst != md5_src
            if modified:
                result = copy_file(src_filepath, dst_filepath)

        self._upload_message(result, dst_filepath, modified)
        return result


########################################################################
# UTILITY FUNCTIONS
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


def create_directory(dirpath):
    """
    Create `dirpath` if it doesn't exist.
    """
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
        _print("INFO", 'New directory "{}" created.'.format(dirpath))


def copy_file(src_filepath, dst_filepath):
    """
    Copy the file at `src` to `dst`.
    """
    # Make sure the directory exists.
    create_directory(os.path.dirname(dst_filepath))

    # Copy the file across.
    try:
        copyfile(src_filepath, dst_filepath)
    except Exception as r:
        print "EXCEPTION:", r
        return False
    return True




########################################################################
if __name__ == "__main__":
    pass
