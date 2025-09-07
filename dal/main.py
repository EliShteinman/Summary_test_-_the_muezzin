import asyncio
import logging

import config
from utilities.kafka.async_client import KafkaProducerAsync
#from utilities.kafka.kafka_utils import AsyncKafkaProducer
from data_load import DataLoad
logging.basicConfig(level=config.LOG_LEVEL)
logging.getLogger("kafka").setLevel(level=config.LOG_KAFKA)
logger = logging.getLogger(__name__)


async def main():
    logger.info("Starting retriever service...")
    logger.info(
        f"Initializing Kafka producer - Server: {config.KAFKA_URL}:{config.KAFKA_PORT}"
    )
    data_loader = DataLoad()
    boostrap_servers = f"{config.KAFKA_PROTOKOL}://{config.KAFKA_URL}:{config.KAFKA_PORT}"
    producer = KafkaProducerAsync(
        bootstrap_servers=boostrap_servers
    )

    try:
        await producer.start()
        logger.info("Kafka producer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}")
        return

    logger.info("Starting main processing loop...")

    while True:
        try:
            for meta_data in data_loader.load_meta_data():
                result = await producer.send_message(
                    config.KAFKA_TOPIC, meta_data
                )
                logger.debug(f"Message sent: {result}")
            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")





if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Retriever service stopped by user")
    except Exception as e:
        logger.critical(f"Critical error in main: {e}")
        raise
    # main()