from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Callable, Dict, Optional

from utilities.elasticsearch.elasticsearch_service import ElasticsearchService
from utilities.logger import Logger

logger = Logger.get_logger()


class ElasticSearchRepository:
    def __init__(self, es: ElasticsearchService):
        self.es_repository = es

    async def generic_enrich_documents(
        self,
        analyzer_func: Callable[[str], Any],
        search_params: Dict[str, Any],
        process_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Generic method for enriching documents with analyzed data.

        Args:
            analyzer_func: Function that analyzes text and returns result
            search_params: Parameters for filtering documents to process
            process_name: Name for logging purposes
        """
        docs_to_process = await self.es_repository.count(**search_params)
        if docs_to_process == 0:
            logger.info(f"No documents to process for {process_name}")
            return None

        logger.info(f"Processing {docs_to_process} documents for {process_name}")

        async def generate_update_actions() -> AsyncGenerator[Dict[str, Any], None]:
            stream = self.es_repository.stream_all_documents(
                fields_to_include=["full_text"], **search_params
            )

            processed_count = 0
            async for doc in stream:
                text_to_analyze = doc["_source"].get("text")
                if not text_to_analyze:
                    continue

                analyzed_result: dict = analyzer_func(text_to_analyze)
                if analyzed_result:
                    yield {
                        "_op_type": "update",
                        "_index": self.es_repository.index_name,
                        "_id": doc["_id"],
                        "doc": {
                            **analyzed_result,
                            "updated_at": datetime.now(timezone.utc),
                        },
                    }
                    processed_count += 1

                    if processed_count % 100 == 0:  # Log progress
                        logger.info(
                            f"Processed {processed_count} documents for {process_name}"
                        )

        result = await self.es_repository.bulk_update(generate_update_actions())
        logger.info(f"Completed {process_name}: {result}")
        return result
