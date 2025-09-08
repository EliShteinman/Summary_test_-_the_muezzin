import hashlib

import config
from utilities.elasticsearch_service import ElasticsearchService, logger
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient
from gridfs import AsyncGridFSBucket

class Proses:
    def __init__(self,mongo_service: MongoDBAsyncClient, elasticsearch: ElasticsearchService):
        self.mongodb = mongo_service
        self.elastic = elasticsearch
        self.db = self.mongodb.get_db()
        self.bucket = AsyncGridFSBucket(self.db, bucket_name=config.MONGO_COLLECTION_NAME)
        self.collection = self.mongodb.get_collection(config.MONGO_COLLECTION_NAME)

    async def proses(self, data:dict):
        path = data['file_path']
        meta_data = data['meta_data']
        file_hash = self.get_file_hash(path)
        meta_data['file_hash'] = file_hash
        meta_data['contentType'] = f"audio/{meta_data['file_suffix']}"
        logger.debug(f"meta_data: {meta_data}")
        await self.elastic.create_document(meta_data)
        logger.debug(f"File {path} uploaded successfully")
        logger.debug(f"File hash: {file_hash}")
        await self.upload_file(path, file_hash, meta_data)
        return meta_data

    def get_file_hash(self, file_path, algorithm='sha256', buffer_size=65536):
        try:
            logger.info(f"Calculating hash for file {file_path}")
            hasher = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(buffer_size)
                    if not data:
                        break
                    hasher.update(data)
            logger.info(f"Hash for file {file_path}: {hasher.hexdigest()}")
            return hasher.hexdigest()
        except FileNotFoundError:
            return f"Error: File not found at {file_path}"
        except Exception as e:
            return f"An error occurred: {e}"

    async def upload_file(self, file_path, file_hash, meta_data):
        async with self.bucket.open_upload_stream(
                filename=file_hash,
                chunk_size_bytes=1048576,
                metadata=meta_data
        ) as grid_in:
            await grid_in.write(open(file_path, 'rb'))


