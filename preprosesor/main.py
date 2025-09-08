import asyncio
import logging

from elasticsearch import AsyncElasticsearch

import config
from preprosesor.proses import Proses
from utilities.elasticsearch_service import ElasticsearchService
from utilities.kafka.async_client import KafkaConsumerAsync
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient

logging.basicConfig(level=config.LOG_LEVEL)

# set logging level for third-party libraries
logging.getLogger("pymongo").setLevel(level=config.LOG_MONGO)
logging.getLogger("kafka").setLevel(level=config.LOG_KAFKA)

logger = logging.getLogger(__name__)


async def main(
    boostrap_servers: str,
    topics: list,
    group_id: str,
    mongo_uri: str,
    mongo_db_name: str,
    collections_name: str,
    es_url: str,
    es_index: str,
):
    logger.info("Starting persister service")

    consumer = KafkaConsumerAsync(
        topics=topics,
        bootstrap_servers=boostrap_servers,
        group_id=group_id,
    )

    mongo_service = MongoDBAsyncClient(
        mongo_uri,
        mongo_db_name,
    )
    await mongo_service.connect()
    logger.info("Connected to MongoDB")
    es_client = AsyncElasticsearch(es_url)
    es_services = ElasticsearchService(es_client, es_index)
    await es_services.initialize_index()
    logger.info("Elasticsearch index initialized")

    proses = Proses(mongo_service, es_services)

    try:
        await consumer.start()
        logger.info("Kafka consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumer: {e}")
        return

    logger.info("Starting main processing loop")

    try:
        while True:
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

    except KeyboardInterrupt:
        logger.info("Shutting down persister service by user request")
    finally:
        try:
            await consumer.stop()
            logger.info("Kafka consumer stopped")
        except Exception as e:
            logger.error(f"Error stopping Kafka consumer: {e}")

        logger.info("Persister service stopped")


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
        es_index = config.ELASTICSEARCH_INDEX

        asyncio.run(
            main(
                boostrap_servers,
                topics,
                group_id,
                mongo_uri,
                mongo_db_name,
                collections_name,
                es_url,
                es_index,
            )
        )
    except Exception as e:
        logger.critical(f"Critical error in main: {e}")
        raise
