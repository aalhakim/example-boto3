# example-boto3

This directory contains example code for the AWS boto3 Python module.

---

## Configuration

To use this code you should install the AWS CLI.

- For Windows: Download the installer from [here [1]](https://aws.amazon.com/cli/)
- For Mac OS: See the installer options [here](https://aws.amazon.com/cli/)
- for Linux: See the installer options [here](https://aws.amazon.com/cli/)
- For Raspberry Pi: Use `sudo apt install awscli`

### Setting System Credentials

Once the tool is installed run it in a command line with `aws configure`.

> *For Windows this works in Command Prompt, PowerShell and Windows Terminal.*

After inserting your details the awscli utility will save your credentials in `~/.aws/credentials` (Linux and Mac) or `%USERPROFILE%\.aws\credentials` (Windows). The outcome should look like:

``` INI
[default]
AWS_ACCESS_KEY = AKIAIOSFODNN7EXAMPLE
AWS_SECRET_KEY = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

For more information on AWS credentials management go [here [2]](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#best-practices-for-configuring-credentials). This site explains how credentials management can be achieved in a variety of ways if the `aws configure` method does not meet your needs.

---

## Editing System Credentials

You can change any pre-set credentials by running `aws configure` again.

### Multiple Profiles

If you want to use more than one set of credentials (e.g. for development one set with write privileges and another with read-only may be convenient) then this can be achieved with profiles.

A profile can be created by running `aws configure --profile profilename`. This will then create the following output in the hidden shared credential file (assuming default settings were previously already set too).

``` INI
[default]
AWS_ACCESS_KEY = AKIAIOSFODNN7EXAMPLE
AWS_SECRET_KEY = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

[profilename]
AWS_ACCESS_KEY = AKIAI44QH8DHBEXAMPLE
AWS_SECRET_KEY = je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY
```

---

## How to Use with Python

To use these configured credentials in Python you simply create a session or client as normal but don't provide any credential information to the object handler. The boto3 module will automatically look through all possible locations that credentials may be stored and should find what has been saved in `.aws/credentials`.

For example

``` python
from boto3.session import Session

session = Session()
s3Client = Resource("s3")

bucket_name = "my-s3-bucket"
s3Client.Bucket(bucket_name)
s3_object = s3Client.Object(bucket_name, path)
```

If you want to use credentials for a specific profile then you will need to make the following change:

``` python
from boto3.session import Session

session = Session(profile_name="profilename")
s3Client = Resource("s3")
```

If you have not configured credentials as above then you may need to manually insert the credentials like so:

``` python
from boto3.session import Session

session = Session()
s3Client = Resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
```
