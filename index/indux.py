import config
from utilities.elasticsearch.elasticsearch_service import ElasticsearchService


class Index:
    def __init__(self, es: ElasticsearchService):
        self.es = es

    async def index_document(self, document: dict, key: str):
        document = dict(document)
        result = await self.es.update_document(
            doc_id=key,
            update_data=document,
        )
        return result
