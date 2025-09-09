from gridfs import AsyncGridFS

import config
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient


class MongoService:
    def __init__(self, mongo_client: MongoDBAsyncClient):
        self.mongo_client = mongo_client
        self.db = self.mongo_client.get_db()
        self.fs = AsyncGridFS(self.db, collection=config.STORAGE_MONGO_COLLECTION_NAME)

    async def upload_file(self, file_path: str, file_hash: str):
        result = await self.fs.put(data=open(file_path, "rb"), _id=file_hash)
        return result
