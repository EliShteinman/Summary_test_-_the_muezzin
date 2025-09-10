import hashlib

import config
from utilities.kafka.async_client import KafkaProducerAsync
from utilities.logger import Logger

logger = Logger.get_logger()


class Proses:
    def __init__(self, producer: KafkaProducerAsync):
        self.producer = producer

    async def proses(self, data: dict):
        path = data["value"]["data"]["file_path"]
        meta_data = data["value"]["data"]["meta_data"]
        file_hash = self._get_file_hash(path)
        meta_data["file_hash"] = file_hash
        meta_data["contentType"] = f"audio/{meta_data['file_suffix']}"
        result_es = await self.producer.send_message(
            topic=config.PREPROCESSOR_KAFKA_TOPIC_OUT_TO_INDEX,
            key=file_hash,
            message=meta_data,
        )
        logger.debug(f"result_es: {result_es}")
        result_mongo = await self.producer.send_message(
            topic=config.PREPROCESSOR_KAFKA_TOPIC_OUT_TO_STORAGE,
            key=file_hash,
            message=path,
        )
        logger.debug(f"result_mongo: {result_mongo}")
        result_transcription = await self.producer.send_message(
            topic=config.PREPROCESSOR_KAFKA_TOPIC_OUT_TO_TRANSCRIPTION,
            key=file_hash,
            message=path,
        )
        logger.debug(f"result_transcription: {result_transcription}")
        return result_es, result_mongo, result_transcription

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
