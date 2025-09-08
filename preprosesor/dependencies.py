from logger import Logger
import logging
from typing import Optional

from elasticsearch import AsyncElasticsearch

import config
from utilities.elasticsearch_service import ElasticsearchService
from utilities.kafka.async_client import KafkaConsumerAsync
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient

# set logging level for third-party libraries
logging.getLogger("pymongo").setLevel(level=config.LOG_MONGO)
logging.getLogger("kafka").setLevel(level=config.LOG_KAFKA)

logger = Logger.get_logger()


_consumer: Optional[KafkaConsumerAsync] = None
_es: Optional[ElasticsearchService] = None
_mongo: Optional[MongoDBAsyncClient] = None


def set_consumer(
    topics: list[str],
    bootstrap_servers: str,
    group_id: str,
):
    logger.info("Setting Kafka consumer")
    logger.debug(
        f"Consumer start with topics: {topics}, bootstrap_servers: {bootstrap_servers}, group_id: {group_id}"
    )
    global _consumer
    _consumer = KafkaConsumerAsync(
        topics=topics,
        bootstrap_servers=bootstrap_servers,
        group_id=config.KAFKA_GROUP_ID,
    )
    return True


def get_consumer():
    logger.info("Getting Kafka consumer")
    global _consumer
    if _consumer is None:
        logger.error("Kafka consumer is not set")
        raise RuntimeError("Kafka consumer is not set")
    return _consumer


def set_es(
    es_url: str,
    es_index: str,
):
    global _es
    logger.info("Setting Elasticsearch client")
    logger.debug(f"Elasticsearch start with es_url: {es_url}, es_index: {es_index}")
    es_client = AsyncElasticsearch(es_url)
    _es = ElasticsearchService(es_client, es_index)
    return True


def get_es():
    if _es is None:
        logger.error("Elasticsearch client is not set")
        raise RuntimeError("Elasticsearch client is not set")
    return _es


def set_mongo(
    mongo_uri: str,
    mongo_db_name: str,
):
    logger.info("Setting MongoDB client")
    logger.debug(
        f"MongoDB start with mongo_uri: {mongo_uri}, mongo_db_name: {mongo_db_name}"
    )
    global _mongo
    _mongo = MongoDBAsyncClient(
        mongo_uri,
        mongo_db_name,
    )
    return True


def get_mongo():
    if _mongo is None:
        logger.error("MongoDB client is not set")
        raise RuntimeError("MongoDB client is not set")
    return _mongo


async def cleaning_resources():
    global _consumer, _es, _mongo
    if _consumer:
        try:
            await _consumer.stop()
            _consumer = None
        except Exception as e:
            logger.error(f"Error stopping Kafka consumer: {e}")
            raise
    if _es:
        _es = None

    if _mongo:
        try:
            await _mongo.close()
            _mongo = None
        except Exception as e:
            logger.error(f"Error stopping MongoDB client: {e}")
            raise
