import boto3
from helpers import decr
from config import s3_bucket_name, s3_access_key, s3_secret_key, s3_region_name, s3_url
from logsys import LogManager


class S3Storage(object):
    def __init__(self):
        self.client = None
        self.logman = LogManager().client

    def __del__(self):
        del self.client

    def connect(self):
        connected = False

        if self.client.__class__.__name__ == 'S3':
            connected = True
        else:
            try:
                self.client = boto3.client(
                    service_name='s3',
                    region_name=s3_region_name,
                    use_ssl=True,
                    endpoint_url=s3_url,
                    aws_access_key_id=s3_access_key,
                    aws_secret_access_key=decr(s3_secret_key)
                )
                connected = True
            except Exception:
                self.client = None

        return connected

    def get_sorted_folder_files(self, folder: str) -> list:
        folder_files = []

        if self.connect():
            try:
                bucket_list = self.client.list_objects_v2(Bucket=s3_bucket_name, Prefix=folder)
            except Exception:
                bucket_list = [{'ETag': '', 'Key': ''}]

            folder_files = [
                kv.get('Key', '').replace('/_SUCCESS', '')
                for kv in sorted(filter(lambda kv: len(kv.get('ETag', '')) > 0 and '/_SUCCESS' in kv.get('Key', ''),
                                        bucket_list['Contents']),
                                 key=lambda kv: kv.get('LastModified'),
                                 reverse=True)
            ]

        return folder_files

    def drop_files(self, file_names: list) -> bool:
        objects = [{'Key': fn} for fn in file_names]
        response = self.client.delete_objects(
            Bucket=s3_bucket_name,
            Delete={
                'Objects': objects,
                'Quiet': True
            },
        )

        res = response.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) == 200

        return res

    def get_files_by_batches(self, folder: str) -> list:
        folder_files = []

        if self.connect():
            try:
                bucket_list = self.client.list_objects_v2(Bucket=s3_bucket_name, MaxKeys=1, Prefix=folder)
                folder_files += [item['Key'] for item in bucket_list['Contents']]
            except Exception as ex:
                self.logman.info('message', f'Reading files from s3 finished with error {ex}. Folder: {folder}')
                return folder_files

            batch_size = 1000
            while folder_files and batch_size == 1000:
                try:
                    bucket_list = self.client.list_objects_v2(Bucket=s3_bucket_name, MaxKeys=1000,
                                                              Prefix=folder, StartAfter=folder_files[-1])
                except Exception as ex:
                    bucket_list = {'Contents': []}

                if ('Contents' in bucket_list) and bucket_list['Contents']:
                    folder_files += [item['Key'] for item in bucket_list['Contents']]

                batch_size = len(bucket_list['Contents']) if 'Contents' in bucket_list else 0

        return folder_files
