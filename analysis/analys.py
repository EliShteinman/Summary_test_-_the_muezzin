from utilities.logger import Logger
from utilities.kafka.async_client import KafkaProducerAsync
import config
import enum
logger = Logger.get_logger()


class AnalysisType(enum.Enum):
    HOSTILE = 1
    LESS_HOSTILE = 2


class Analysis:
    def __init__(self,producer: KafkaProducerAsync, hostile_words, less_hostile_words):
        self.producer = producer
        self.hostile_words = set(hostile_words)
        self.less_hostile_words = set(less_hostile_words)


    def analysis(self, data: dict, key: str):
        logger.debug(f"Analysis data: {data}")
        text_to_analyze = data['value']['data']["full_text"].lower()
        hostile_count = sum(word in text_to_analyze for word in self.hostile_words)
        less_hostile_count = sum(word in text_to_analyze for word in self.less_hostile_words)
