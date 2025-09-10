from gridfs import AsyncGridFS

import config
from utilities.logger import Logger
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient

logger = Logger.get_logger()


class MongoService:
    def __init__(self, mongo_client: MongoDBAsyncClient):
        self.mongo_client = mongo_client
        self.db = self.mongo_client.get_db()
        self.fs = AsyncGridFS(self.db, collection=config.STORAGE_MONGO_COLLECTION_NAME)

    async def upload_file(self, file_path: str, file_hash: str):
        """Upload a file to MongoDB"""
        file_exists = await self.fs.exists(_id=file_hash)
        if file_exists:
            logger.debug(f"File with hash {file_hash} already exists, skipping upload")
            return file_hash
        logger.debug(f"Uploading file: {file_path}, with hash: {file_hash}")
        result = await self.fs.put(data=open(file_path, "rb"), _id=file_hash)
        logger.debug(
            f"Uploaded file: {file_path}, with hash: {file_hash}, result: {result}"
        )
        return result
