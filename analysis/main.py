import asyncio
import time
from utilities.encoding import Encoding
import config
from utilities.kafka.async_client import KafkaConsumerAsync, KafkaProducerAsync
from utilities.logger import Logger
from analys import Analysis

logger = Logger.get_logger()


async def main():
    logger.info("Starting analyser service...")
    bootstrap_servers = (
        rf"{config.ANALYZER_KAFKA_HOST}:{config.ANALYZER_KAFKA_PORT}"
    )

    # Initialize Kafka producer and consumer
    producer = KafkaProducerAsync(
        bootstrap_servers=bootstrap_servers,
    )

    try:
        await producer.start()
        logger.info("Kafka producer and consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka: {e}")
        return

    consumer = KafkaConsumerAsync(
        [config.ANALYZER_KAFKA_TOPIC_IN],
        bootstrap_servers=bootstrap_servers,
        group_id=config.ANALYZER_KAFKA_GROUP_ID,
    )

    try:
        await producer.start()
        await consumer.start()
        logger.info("Kafka producer and consumer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka: {e}")
        return

    encodings = Encoding()
    hostile = encodings.decode_base64(config.ANALYZER_HOSTILE_WORDS)
    hostile = hostile.split(",")
    less_hostile = encodings.decode_base64(config.ANALYZER_LESS_HOSTILE_WORDS)
    less_hostile = less_hostile.split(",")

    analysis = Analysis(
        producer,
        less_hostile,
        hostile,
    )
    # Performance tracking variables
    message_count = 0
    processed_in_batch = 0
    last_stats_time = time.time()
    logger.info("Starting main processing loop")

    while True:
        try:
            async for data in consumer.consume():
                logger.debug(f"Received data: {data}")
                topic = data["topic"]
                key = data["key"]
                data = data['value']
                message_count += 1
                processed_in_batch += 1

                logger.debug(
                    f"Processing message #{message_count} from topic '{topic}'"
                )

                # Track processing time for each message
                process_start_time = time.time()
                result = analysis.analysis(
                    data=data,
                    key=key,
                )
                processing_time = time.time() - process_start_time
                logger.debug(f"Result: {result}")
                logger.info(f"Processed file {key} in {processing_time:.3f}s")

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
