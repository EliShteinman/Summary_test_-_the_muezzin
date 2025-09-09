import asyncio
import time

from elasticsearch import AsyncElasticsearch
from indux import Index

import config
from utilities.elasticsearch.elasticsearch_service import ElasticsearchService
from utilities.kafka.async_client import KafkaConsumerAsync
from utilities.logger import Logger
from utilities.files.data_loader_client import UniversalDataLoader
logger = Logger.get_logger()


async def main():
    logger.info("Starting indexer service...")

    # Initialize Kafka consumer
    consumer = KafkaConsumerAsync(
        [config.INDEXER_KAFKA_TOPIC_IN],
        bootstrap_servers=f"{config.INDEXER_KAFKA_HOST}:{config.INDEXER_KAFKA_PORT}",
        group_id=config.INDEXER_KAFKA_GROUP_ID,
    )

    try:
        await consumer.start()
        logger.info("Kafka producer and consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka: {e}")
        return
    dal = UniversalDataLoader()
    try:
        mapping = dal.load_json_as_dict(
            r'mapping.json'
        )
    except Exception as e:
        logger.error(f"Failed to load mapping: {e}")
        return

    try:
        es_url = f"{config.INDEXER_ELASTICSEARCH_PROTOCOL}://{config.INDEXER_ELASTICSEARCH_HOST}:{config.INDEXER_ELASTICSEARCH_PORT}"
        es_client = AsyncElasticsearch(es_url)
        es = ElasticsearchService(es_client, config.INDEXER_ELASTICSEARCH_INDEX_DATA)
        await es.initialize_index(mapping=mapping)
        logger.info("Elasticsearch client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Elasticsearch client: {e}")
        return

    ind = Index(es)

    # Performance tracking variables
    message_count = 0
    processed_in_batch = 0
    last_stats_time = time.time()
    logger.info("Starting main processing loop")
    while True:
        try:
            async for result in consumer.consume():
                logger.debug(f"Received data: {result}")
                topic = result["topic"]
                file = result["value"]['data']
                key = result["key"]
                message_count += 1
                processed_in_batch += 1
                file_id = file.get("file_hash", "unknown_id")

                logger.debug(
                    f"Processing message #{message_count} from topic '{topic}'"
                    f" - File ID: {file_id}, Key: {key}"
                )

                # Track processing time for each message
                process_start_time = time.time()
                try:
                    result = await ind.index_document(file, key)
                    logger.debug(f"Result: {result}")
                except Exception as e:
                    logger.error(f"Error indexing document: {e}")
                    logger.info(f"Failed to index document {file_id}")
                processing_time = time.time() - process_start_time
                logger.debug(f"Result: {result}")
                logger.info(f"Processed file {file_id} in {processing_time:.3f}s")

                # Print statistics every 60 seconds
                current_time = time.time()
                if current_time - last_stats_time > 60:
                    rate = processed_in_batch / 60
                    logger.info(
                        f"Processing rate: {rate:.2f} messages/second | Total processed: {message_count}"
                    )

                    last_stats_time = current_time
                    processed_in_batch = 0

                # Log every 100 messages for general tracking
                if message_count % 100 == 0:
                    logger.info(f"Milestone: Processed {message_count} total messages")

        except Exception as e:
            logger.error(f"Error consuming messages from Kafka: {e}")

        # Sleep between batches to prevent overwhelming the system
        await asyncio.sleep(5)
    #


if __name__ == "__main__":
    try:
        logger.info("Application startup initiated")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Enricher service stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in main: {e}")
        raise
    finally:
        logger.info("Application shutdown complete")
