import asyncio
import time

from mongo_service import MongoService

import config
from utilities.kafka.async_client import KafkaConsumerAsync
from utilities.logger import Logger
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient

logger = Logger.get_logger()


async def main():
    logger.info("Starting storage service")

    client = MongoDBAsyncClient(config.STORAGE_MONGO_URI, config.STORAGE_MONGO_DB_NAME)
    try:
        await client.connect()
        logger.info("MongoDB client connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return

    # Initialize Kafka consumer
    consumer = KafkaConsumerAsync(
        [config.STORAGE_KAFKA_TOPIC_IN],
        bootstrap_servers=f"{config.STORAGE_KAFKA_HOST}:{config.STORAGE_KAFKA_PORT}",
        group_id=config.STORAGE_KAFKA_GROUP_ID,
    )
    try:
        await consumer.start()
        logger.info("Kafka consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka consumer: {e}")
        return

    service = MongoService(client)

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
                file = result["value"]["data"]
                key = result["key"]
                message_count += 1
                processed_in_batch += 1
                file_id = result["value"]["key"]

                logger.debug(
                    f"Processing message #{message_count} from topic '{topic}' - File ID: {file_id}"
                )
                process_start_time = time.time()
                result = await service.upload_file(file, key)
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
            logger.error(f"Error in consumer loop: {e}")
            logger.info("Attempting to reconnect in 5 seconds")
        await asyncio.sleep(5)


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
