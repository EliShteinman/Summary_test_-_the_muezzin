import asyncio
from datetime import datetime

from analys import Analysis
from elasticsearch import AsyncElasticsearch

import config
from utilities.elasticsearch.elasticsearch_service import ElasticsearchService
from utilities.encoding import Encoding
from utilities.logger import Logger

logger = Logger.get_logger()


async def main():
    logger.info("Starting analysis service...")
    try:
        es_url = f"{config.ANALYZER_ELASTICSEARCH_PROTOCOL}://{config.ANALYZER_ELASTICSEARCH_HOST}:{config.ANALYZER_ELASTICSEARCH_PORT}"
        es_client = AsyncElasticsearch(es_url)
        es = ElasticsearchService(es_client, config.ANALYZER_ELASTICSEARCH_INDEX_DATA)
        await es.is_connected()
        logger.info("Elasticsearch client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Elasticsearch client: {e}")
        return

    logger.info("Starting encoding process")
    encodings = Encoding()
    hostile = encodings.decode_base64(config.ANALYZER_HOSTILE_WORDS)
    hostile = hostile.split(",")
    logger.debug(f"Hostile words: {hostile}")
    less_hostile = encodings.decode_base64(config.ANALYZER_LESS_HOSTILE_WORDS)
    less_hostile = less_hostile.split(",")
    logger.debug(f"Less hostile words: {less_hostile}")
    logger.info("Encoding process completed")

    analysis = Analysis(
        es_service=es,
        hostile_words=hostile,
        less_hostile_words=less_hostile,
    )
    logger.info("Starting main processing loop")

    while True:
        try:
            await asyncio.create_task(analysis.run_analysis())
        except Exception as e:
            logger.error(f"Error consuming messages from Kafka: {e}")

        logger.debug(f"Sleeping for 60 seconds..., time now {datetime.now()}")
        await asyncio.sleep(60)


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
