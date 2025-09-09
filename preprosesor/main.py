import asyncio
from time import sleep

from dependencies import (get_consumer, get_es, get_mongo, set_consumer,
                          set_es, set_mongo)

import config
from preprosesor.proses import Proses
from utilities.logger import Logger

logger = Logger.get_logger()


async def main(
    boostrap_servers: str,
    topics: list,
    group_id: str,
    mongo_uri: str,
    mongo_db_name: str,
    es_url: str,
    es_index: str,
):
    logger.info("Starting persister service")
    if set_consumer(
        topics=topics,
        bootstrap_servers=boostrap_servers,
        group_id=group_id,
    ):
        logger.info("Kafka consumer set")
    else:
        logger.error("Kafka consumer not set")
        return
    sleep(10)
    consumer = get_consumer()
    logger.debug("Kafka consumer get")

    if set_mongo(
        mongo_uri,
        mongo_db_name,
    ):
        logger.info("MongoDB set")
    else:
        logger.error("MongoDB not set")
        return
    sleep(10)
    mongo_service = get_mongo()
    logger.debug("MongoDB client get")

    await mongo_service.connect()
    logger.info("Connected to MongoDB")

    if set_es(es_url, es_index):
        logger.info("Elasticsearch set")
    else:
        logger.error("Elasticsearch not set")
        return
    sleep(10)
    es_services = get_es()
    logger.debug("Elasticsearch client get")
    await es_services.initialize_index()
    logger.info("Elasticsearch index initialized")

    proses = Proses()

    try:
        await consumer.start()
        logger.info("Kafka consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumer: {e}")
        return

    logger.info("Starting main processing loop")
    try:
        async for data in consumer.consume():
            try:
                logger.info(f"Processing podcast: {data['value']['data']}")
                result = await proses.proses(data["value"]["data"])
                logger.info(f"Podcast processed: {result}")
            except Exception as e:
                logger.error(f"Error processing podcast: {e}")
                continue

    except Exception as e:
        logger.error(f"Error in consumer loop: {e}")
        logger.info("Attempting to reconnect in 5 seconds")
        await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        boostrap_servers = f"{config.KAFKA_URL}:{config.KAFKA_PORT}"
        topics = [config.KAFKA_INPUT_TOPIC]
        group_id = config.KAFKA_GROUP_ID
        mongo_uri = config.MONGO_URI
        mongo_db_name = config.MONGO_DB_NAME
        collections_name = config.MONGO_COLLECTION_NAME
        es_protocol = config.ELASTICSEARCH_PROTOCOL
        es_host = config.ELASTICSEARCH_HOST
        es_port = config.ELASTICSEARCH_PORT
        es_url = f"{es_protocol}://{es_host}:{es_port}"
        es_index = config.ELASTICSEARCH_INDEX_DATA

        asyncio.run(
            main(
                boostrap_servers,
                topics,
                group_id,
                mongo_uri,
                mongo_db_name,
                es_url,
                es_index,
            )
        )
    except Exception as e:
        logger.critical(f"Critical error in main: {e}")
        raise
