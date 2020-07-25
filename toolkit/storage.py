"""Storage utility to work with S3, GCS and minio.

HOW TO USE:
* Make sure that credentials are configured as env variables.
* Expected environment variables are
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_ENDPOINT_URL(AWS S3 endpoint is used if not provided)
    - AWS_DEFAULT_REGION (Required if AWS_ENDPOINT_URL is not present)
* To use this with module with GCS
    - Generate GCS HMAC credentials and set them as aws credentials.
    - Please make sure that AWS_ENDPOINT_URL is set to 'https://storage.googleapis.com'
"""
from __future__ import annotations
import os
import io
import boto3
import pandas as pd
import botocore
import tempfile
import functools
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def _get_config(name, default=None, required=True):
    value = os.environ.get(name, default)
    if required and value is None:
        raise Exception(f"missing configuration: {name}")
    return value

def _get_aws_settings(
        access_key_env_name = "AWS_ACCESS_KEY_ID",
        secret_access_key_env_name = "AWS_SECRET_ACCESS_KEY",
        endpoint_url_env_name="AWS_ENDPOINT_URL",
        region_name_env_name="AWS_DEFAULT_REGION"):

    aws_settings = dict(
        aws_access_key_id =  _get_config(access_key_env_name),
        aws_secret_access_key = _get_config(secret_access_key_env_name)
    )

    endpoint_url = _get_config(endpoint_url_env_name, required=False)
    if endpoint_url:
        aws_settings['endpoint_url'] = endpoint_url
    else:
        aws_settings['region_name'] = _get_config(region_name_env_name, "US")

    return aws_settings

def get_s3(aws_settings):
    return boto3.resource("s3", **aws_settings)

class FileNotFoundError(Exception):
    pass

def _exception_handler(func):
    """Decorator to handle the exceptions occur in StoragePath class.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] in ["404", "NoSuchKey"]:
                raise FileNotFoundError(self.path)
            else:
                raise
    return wrapper

class StoragePath:
    """The StoragePath class provides a pathlib.Path like interface for
    storage.
    USAGE:
        root = StoragePath(bucket_name, "alpha")
        path = root / "datasets" / "customer-master" / "template.csv"
        text = path.read_text()
    """
    def __init__(self, path: str, bucket: str=None, aws_settings: dict=None):
        self.path = path.strip('/')
        self.bucket = bucket or _get_config("STORAGE_BUCKET")
        self.aws_settings = aws_settings or _get_aws_settings()
        self.s3 = get_s3(self.aws_settings)

    @property
    def _object(self):
        """Returns a storage object resource.
        """
        return self.s3.Object(bucket_name=self.bucket, key=self.path)

    @property
    def size(self) -> int:
        """Returns the size of the object in bytes
        """
        return self._object.content_length

    @property
    def md5_hash(self):
        """Returns the md5_hash of the blob in base64 encoded form
        TODO: Is this needed?
        """
        pass

    @property
    def name(self) -> str:
        return self.path.split('/')[-1]

    def exists(self) -> bool:
        """Tells the storage path exists or not.

        Checks if the path exists or not by getting objects metadata.
        """
        obj = self._object
        try:
            obj.metadata
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise

    def dir_exists(self) -> bool:
        """Tells the storage dir exists or not.

        Checks if the dir exists or not by checking files in the directory.
        """
        return any(True for _ in self.iterdir())

    def unlink(self) -> dict:
        """Remove the storage path file.

        NOTE: This returns success response(204), even if the path does not exist.
        """
        logger.info(f'Delete storage Path {self.path}')
        obj = self._object
        return obj.delete()

    def rmdir(self) -> list:
        """Removes the directory with its contents.

        NOTE: This returns success response(204), even if the path does not exist.
        """
        logger.info(f"Delete storage directory {self.path}")
        bucket = self._object.Bucket()
        return bucket.objects.filter(Prefix=self.path).delete()

    @_exception_handler
    def download(self, local_path):
        """Download the contents of storage file to the local_path file.
        """
        logger.info(f"Download from storage path {self.path} to local {local_path}")
        obj = self._object
        obj.download_file(local_path)

    def upload(self, local_path):
        """Uploads the file from local_path to storage path.
        """
        logger.info(f"Upload from local path {local_path} to storage path {self.path}")
        obj = self._object
        obj.upload_file(local_path)

    @_exception_handler
    def read_bytes(self) -> bytes:
        """Read the contents from this path as bytes.
        """
        logger.info(f"Read bytes from path: {self.path}")
        obj = self._object
        return obj.get()['Body'].read()

    @_exception_handler
    def read_text(self) -> str:
        """Read the contents from this path as string.
        """
        logger.info(f"Read text from path: {self.path}")
        return self.read_bytes().decode('utf-8')

    @_exception_handler
    def read_csv(self, **kwargs) -> pd.DataFrame:
        """Read the contents of this csv file as dataframe.
        """
        logger.info(f"Read csv from path: {self.path}")
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmpfile:
            self.download(tmpfile.name)
            df = pd.read_csv(tmpfile.name, **kwargs)
            return df

    @_exception_handler
    def read_parquet(self, **kwargs) -> pd.DataFrame:
        """Read the contents of this parquet file as dataframe.
        """
        logger.info(f"Read parquet from path: {self.path}")
        with tempfile.NamedTemporaryFile(suffix='.parq') as tmpfile:
            self.download(tmpfile.name)
            df = pd.read_parquet(tmpfile.name)
            return df

    def write_bytes(self, data):
        """Writes provided data into the storage path.
        """
        logger.info(f"Write bytes to path: {self.path}")
        obj = self._object
        obj.upload_fileobj(io.BytesIO(data))

    def write_text(self, text):
        """Writes provided text into the storage path.
        """
        logger.info(f"Write text to path: {self.path}")
        data = text.encode('utf-8')
        self.write_bytes(data)

    def write_csv(self, df: pd.DataFrame):
        """Writes given dataframe into the storage path in csv format.
        """
        logger.info(f"Write df to storage path as csv: {self.path}")
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmpfile:
            df.to_csv(tmpfile.name, index=False)
            self.upload(tmpfile.name)

    def write_parquet(self, df: pd.DataFrame):
        """Writes given dataframe into the storage path in parq format.
        """
        logger.info(f"Write df to storage path as parquet: {self.path}")
        with tempfile.NamedTemporaryFile(suffix='.parq') as tmpfile:
            df.to_parquet(tmpfile.name, index=False)
            self.upload(tmpfile.name)

    def iterdir(self):
        """List files in that path.

        TODO: Check if there is a limit in number of results.
        """
        directory = self.path + "/"
        logger.info(f"Get files from directory: {directory}")
        bucket = self._object.Bucket()
        collection = bucket.objects.filter(Prefix=directory)
        return (StoragePath(obj.key, self.bucket, self.aws_settings) for obj in collection.all())

    def sync_to(self, local_path: str):
        """recursively copies the contents of remote tree to local directory

        >>> path = StoragePath(/alpha/tables', 'bucket_name')
        >>> path.sync_to('/tmp/tables')
        """
        logger.info(f"Sync storage files {self.path} to local {local_path}")
        storage_files = self.iterdir()  # Get the storage files list
        for file in storage_files:
            prefix = Path(local_path)  # Prefix for local file path

            # local file path suffix comes from the storage file path.
            # suffix is split into parts to make it work with all os systems.
            suffix_parts = file.path.replace(self.path, '', 1).strip('/').split('/')
            local_file_path = prefix.joinpath(*suffix_parts)

            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            file.download(str(local_file_path))

    def sync_from(self, local_path: str):
        """Recursively copies the directory tree at local_path to storage.

        >>> path = StoragePath('bucket_name', '/alpha/uploads')
        >>> path.sync_from('/tmp/uploads')
        """
        logger.info(f"Sync local files {local_path} to storage {self.path}")
        # Get all the files from local_path
        files = [each for each in Path(local_path).glob("**/*") if each.is_file()]
        for file in files:
            storage_path = self.joinpath(*file.relative_to(local_path).parts)
            storage_path.upload(str(file))

    def copy(self, source_path: StoragePath):
        """Copy the file from source storage path to at this storage path.
        """
        logger.info(f"copy within storage from {source_path.path} to {self.path}")
        obj = self._object
        obj.copy(source_path.dict())

    def copy_dir(self, source_dir: StoragePath):
        """Recursively copies the source_dir storage path tree to at this storage path.

        source/uploads -> dest/uploads
        """
        logger.info(f"copy dir within strorage from {source_dir.path} to {self.path}")
        files = source_dir.iterdir()
        for source_path in files:
            dest_file = source_path.path.replace(source_dir.path, self.path, 1)
            dest_path = StoragePath(dest_file, self.bucket, self.aws_settings)
            dest_path.copy(source_path)

    def move(self, source_path):
        """Move file from source storage path to at this storage path.
        """
        logger.info(f"Move within storage from {source_path.path} to {self.path}")
        self.copy(source_path)
        source_path.unlink()

    def move_dir(self, source_dir):
        """Recursively moves the  source_dir storage path tree to at this storage path.
        """
        logger.info(f"Move dir within strorage from {source_dir.path} to {self.path}")
        self.copy_dir(source_dir)
        source_dir.rmdir()

    def _get_presigned_url(self, client_method, expires=600, content_type=None):
        """Returns a presigned URL for upload or download.
        The client_method should be one of get_object or put_object.
        """
        params = self.dict()
        if content_type:
            params['ContentType'] = content_type

        return self.s3.meta.client.generate_presigned_url(client_method,
            Params=params,
            ExpiresIn=expires
        )

    def get_presigned_url_for_download(self, expires=3600):
        """Returns a presigned URL for upload.

        The default expiry is one hour (3600 seconds).
        """
        logger.info(f"Get presigned url for download for storage path {self.path}")
        return self._get_presigned_url(client_method='get_object', expires=expires)

    def get_presigned_url_for_upload(self, expires=600, content_type="text/csv"):
        """Returns a presigned URL for upload.

        The default expiry is 10 minutes (600 seconds).
        """
        logger.info(f"Get presigned url for upload for storage path {self.path}")
        return self._get_presigned_url(client_method='put_object', expires=expires, content_type=content_type)

    def joinpath(self, *parts):
        """Combine the storage path with one or more parts and returns a new path.
        """
        path = self
        for p in parts:
            path = path / p
        return path

    def dict(self):
        return {
            'Bucket': self.bucket,
            'Key': self.path,
        }

    def __truediv__(self, path):
        return StoragePath(self.path + "/" + path, self.bucket, self.aws_settings)

    def __repr__(self):
        return f'<StoragePath {self.path}>'

    def __str__(self):
        return self.path

