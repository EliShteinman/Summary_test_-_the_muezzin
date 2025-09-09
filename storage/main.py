import asyncio
from utilities.logger import Logger
import config

from utilities.kafka.async_client import KafkaConsumerAsync
from utilities.mongoDB.mongodb_async_client import MongoDBAsyncClient


logger = Logger.get_logger()


async def main():
    logger.info("Starting persister service")

    client = MongoDBAsyncClient(config.STORAGE_MONGO_URI, config.STORAGE_MONGO_DB_NAME)
    await client.connect()
    logger.info("MongoDB client connected successfully")

    # Initialize collections
    collection = client.get_collection(
        collection_name=config.STORAGE_MONGO_COLLECTION_NAME
    )

    logger.info(
        f"Collections initialized: {config.STORAGE_MONGO_COLLECTION_NAME}"
    )

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

    logger.info("Starting main processing loop")

    # try:
    #     while True:
    #         try:
    #             async for topic, tweet in consumer.consume():
    #                 try:
    #                     if topic == config.KAFKA_TOPIC_ANTISEMITIC:
    #                         tweet_id = await antisemitic_dal.insert_tweet(tweet)
    #                         logger.debug(f"Inserted antisemitic tweet: {tweet_id}")
    #                     elif topic == config.KAFKA_TOPIC_NOT_ANTISEMITIC:
    #                         tweet_id = await normal_dal.insert_tweet(tweet)
    #                         logger.debug(f"Inserted normal tweet: {tweet_id}")
    #                     else:
    #                         logger.warning(f"Unknown topic '{topic}', skipping message")
    #                         continue
    #
    #                 except Exception as e:
    #                     logger.error(f"Failed to insert tweet from topic {topic}: {e}")
    #                     continue
    #
    #         except Exception as e:
    #             logger.error(f"Error in consumer loop: {e}")
    #             logger.info("Attempting to reconnect in 5 seconds")
    #             await asyncio.sleep(5)
    #
    # except KeyboardInterrupt:
    #     logger.info("Shutting down persister service by user request")
    # finally:
    #     try:
    #         await consumer.stop()
    #         logger.info("Kafka consumer stopped")
    #     except Exception as e:
    #         logger.error(f"Error stopping Kafka consumer: {e}")
    #
    #     logger.info("Persister service stopped")
    #

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"Critical error in main: {e}")
        raise
