import logging
from datetime import datetime, timezone

from elasticsearch import Elasticsearch

import config


class Logger:
    _logger = None

    @classmethod
    def get_logger(
        cls,
        name=__name__,
        es_url=f"{config.LOG_ELASTICSEARCH_HOST}://{config.LOG_ELASTICSEARCH_HOST}:{config.LOG_ELASTICSEARCH_PORT}",
        index=config.LOG_ELASTICSEARCH_INDEX_LOG,
        level=logging.DEBUG,
    ):
        if cls._logger:
            return cls._logger
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.handlers:
            es = Elasticsearch(es_url)

            class ESHandler(logging.Handler):
                def emit(self, record):
                    try:
                        es.index(
                            index=index,
                            document={
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "level": record.levelname,
                                "logger": record.name,
                                "message": record.getMessage(),
                            },
                        )
                    except Exception as e:
                        print(f"ES log failed: {e}")

            logger.addHandler(ESHandler())
        logger.addHandler(logging.StreamHandler())
        cls._logger = logger
        return logger
