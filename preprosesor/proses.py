import hashlib
from utilities.elasticsearch_service import ElasticsearchService
from utilities.mongo_utils import SingletonMongoClient
import gridfs

class Proses:
    def __init__(self, mongodb: SingletonMongoClient, elasticsearch: ElasticsearchService):
        self.mongodb = mongodb
        self.elastic = elasticsearch
        self.fs = gridfs.GridFS(self.mongodb.get_collection())
        self.collection = self.mongodb.get_collection()

    async def proses(self, data:dict):
        path = data['file_path']
        meta_data = data['meta_data']
        file_hash = self.get_audio_file_hash(path)
        meta_data['file_hash'] = file_hash
        await self.elastic.create_document(meta_data)
        file_id = self.proses_file(path, file_hash)
        await self.collection.update_one({'file_hash': file_hash}, {'$set': {'file_id': file_id}})
        return file_id


    def get_audio_file_hash(self, file_path, algorithm='sha256', buffer_size=65536):
        """
        Calculates the hash of an audio file.

        Args:
            file_path (str): The path to the audio file.
            algorithm (str): The hashing algorithm to use (e.g., 'md5', 'sha1', 'sha256').
            buffer_size (int): The size of chunks to read the file in (in bytes).

        Returns:
            str: The hexadecimal representation of the file's hash.
        """
        try:
            hasher = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(buffer_size)
                    if not data:
                        break
                    hasher.update(data)
            print(hasher.hexdigest())
            return hasher.hexdigest()
        except FileNotFoundError:
            return f"Error: File not found at {file_path}"
        except Exception as e:
            return f"An error occurred: {e}"


    def proses_file(self, file_path, file_hash):
        file_id = self.fs.put(open(file_path, 'rb'), filename=file_hash)
        return file_id