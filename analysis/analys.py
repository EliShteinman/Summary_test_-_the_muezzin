from utilities.logger import Logger
from utilities.kafka.async_client import KafkaProducerAsync

logger = Logger.get_logger()


class Analysis:
    def __init__(self,producer: KafkaProducerAsync, hostile_words, less_hostile_words):
        self.producer = producer
        self.hostile_words = hostile_words
        self.less_hostile_words = less_hostile_words

    def analysis(self, data: dict):
        logger.debug(f"Analysis data: {data}")
        pass