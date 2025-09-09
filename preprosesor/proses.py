import hashlib
from typing import Optional

from dependencies import get_es, get_mongo
from gridfs import AsyncGridFSBucket
from utilities.logger import Logger
import config
from utilities.elasticsearch_service import ElasticsearchService
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient
from gridfs.asynchronous import AsyncGridFS

logger = Logger.get_logger()

class Proses:
    def __init__(
        self,
        mongo_service: Optional[MongoDBAsyncClient] = None,
        elasticsearch: Optional[ElasticsearchService] = None,
    ):
        self.mongodb = mongo_service or get_mongo()
        self.elastic = elasticsearch or get_es()
        self.db = self.mongodb.get_db()
        self.bucket = AsyncGridFSBucket(
            self.db, bucket_name=config.MONGO_COLLECTION_NAME
        )
        self.collection = self.mongodb.get_collection(config.MONGO_COLLECTION_NAME)
        self.fs = AsyncGridFS(self.db, collection=config.MONGO_COLLECTION_NAME)

    async def proses(self, data: dict):
        path = data["file_path"]
        meta_data = data["meta_data"]
        file_hash = self._get_file_hash(path)
        meta_data["file_hash"] = file_hash
        meta_data["contentType"] = f"audio/{meta_data['file_suffix']}"
        logger.debug(f"meta_data: {meta_data}")
        await self.elastic.create_document(meta_data)
        logger.debug(f"File {path} uploaded successfully")
        logger.debug(f"File hash: {file_hash}")
        await self.upload_file(path, file_hash, meta_data)
        return meta_data

    @staticmethod
    def _get_file_hash(file_path, algorithm="sha256", buffer_size=65536):
        try:
            logger.info(f"Calculating hash for file {file_path}")
            hasher = hashlib.new(algorithm)
            with open(file_path, "rb") as f:
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
        await self.fs.put(open(file_path, "rb"), _id=file_hash, metadata=meta_data)
        return await self.fs.find_one({"_id": file_hash})