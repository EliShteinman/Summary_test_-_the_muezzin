import config
from utilities.kafka.async_client import KafkaProducerAsync
from utilities.logger import Logger
from utilities.sst.whisper_service import WhisperService

logger = Logger.get_logger()


class Transparency:
    def __init__(self, sst: WhisperService, producer: KafkaProducerAsync):
        self.sst = sst
        self.producer = producer

    async def transcribe(self, file_path, file_hash: str, **kwargs):
        logger.info(f"Transcribing file: {file_path}")
        try:
            transcription = self.sst.whisper_transcribe(file_path, file_hash, **kwargs)
            logger.debug(f"Transcription result: {transcription}")
        except Exception as e:
            logger.error(f"Transcription failed: {e}")
            raise
        try:
            result = await self.producer.send_message(
                topic=rf"{config.TR_KAFKA_TOPIC_OUT}",
                key=file_hash,
                message=transcription,
            )
            logger.debug(f"Result: {result}")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            raise
        return result


if __name__ == "__main__":
    transparency = Transparency(
        sst=WhisperService(model_name="tiny", download_root=r"C:\models\whisper"),
        producer=KafkaProducerAsync(
            bootstrap_servers=f"{config.TR_KAFKA_HOST}:{config.TR_KAFKA_PORT}"
        ),
    )
    a = transparency.transcribe(file_path=r"C:\podcasts\download.wav", file_hash="123")
    print(a)
