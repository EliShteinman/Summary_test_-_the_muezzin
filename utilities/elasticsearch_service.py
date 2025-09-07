import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import NotFoundError


logger = logging.getLogger(__name__)

class ElasticsearchService:
    def __init__(self, es: AsyncElasticsearch, index_name: str):
        self.es = es
        self.index_name = index_name
        
    def _create_document_mapping(self) -> Dict[str, Any]:
        """Create optimized mapping for document storage and search"""
        return {
            'properties': {
                'file_hash': {
                    'type': 'keyword',
                },
                'file_suffix': {
                    'type': 'keyword',
                },
                'file_name': {
                    'type': 'keyword'
                },
                'file_size': {
                    'type': 'float'
                },
                'file_creation_time': {
                    'type': 'timestamp',
                },
                'file_modification_time': {
                    'type': 'timestamp',
                },
                'file_access_time': {
                    'type': 'timestamp'
                },
                'file_permissions': {
                    'type': 'timestamp'
                },
            }
        }
    
    async def initialize_index(self) -> None:
        """Initialize the Elasticsearch index with proper mapping"""
        try:
            if not self.es.indices.exists(index=self.index_name):
                logger.info(f"Creating index {self.index_name}")
                await self.es.indices.create(
                    index=self.index_name,
                    mappings=self._create_document_mapping()
                )
                logger.info(f"Index {self.index_name} created successfully")
            else:
                logger.info(f"Index {self.index_name} already exists")
        except Exception as e:
            logger.error(f"Failed to initialize index: {e}")
            raise
    
    async def create_document(self, document):
        """Create a new document"""
        doc_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        
        doc_data = document.dict()
        doc_data.update({
            'created_at': now,
            'updated_at': now
        })
        
        try:
            await self.es.index(
                index=self.index_name,
                id=doc_id,
                body=doc_data
            )
            await self.es.indices.refresh(index=self.index_name)
            
            return await self.get_document(doc_id)
        except Exception as e:
            logger.error(f"Failed to create document: {e}")
            raise
    
    async def get_document(self, doc_id: str):
        """Get a document by ID"""
        try:
            result = await self.es.get(index=self.index_name, id=doc_id)
            source = result['_source']
            return source
        except NotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to get document {doc_id}: {e}")
            raise
    
    # async def update_document(self, doc_id: str, update_data):
    #     """Update a document"""
    #     try:
    #         # First check if document exists
    #         existing = await self.get_document(doc_id)
    #         if not existing:
    #             return None
    #
    #         # Prepare update data
    #         update_dict = {k: v for k, v in update_data.dict().items() if v is not None}
    #         update_dict['updated_at'] = datetime.now(timezone.utc)
    #
    #         self.es.update(
    #             index=self.index_name,
    #             id=doc_id,
    #             body={'doc': update_dict}
    #         )
    #         self.es.indices.refresh(index=self.index_name)
    #
    #         return await self.get_document(doc_id)
    #     except NotFoundError:
    #         return None
    #     except Exception as e:
    #         logger.error(f"Failed to update document {doc_id}: {e}")
    #         raise
    #
    # async def delete_document(self, doc_id: str) -> bool:
    #     """Delete a document"""
    #     try:
    #         self.es.delete(index=self.index_name, id=doc_id)
    #         self.es.indices.refresh(index=self.index_name)
    #         return True
    #     except NotFoundError:
    #         return False
    #     except Exception as e:
    #         logger.error(f"Failed to delete document {doc_id}: {e}")
    #         raise
    #
    # async def search_documents(
    #     self,
    #     query: Optional[str] = None,
    #     category: Optional[str] = None,
    #     tags: Optional[List[str]] = None,
    #     author: Optional[str] = None,
    #     status: Optional[str] = None,
    #     limit: int = 10,
    #     offset: int = 0
    # ):
    #     """Advanced search with multiple filters"""
    #     search_body = {
    #         'query': {'bool': {'must': [], 'filter': []}},
    #         'from': offset,
    #         'size': limit,
    #         'sort': [{'created_at': {'order': 'desc'}}]
    #     }
    #
    #     # Text search
    #     if query:
    #         search_body['query']['bool']['must'].append({
    #             'multi_match': {
    #                 'query': query,
    #                 'fields': ['title^2', 'body'],
    #                 'type': 'best_fields'
    #             }
    #         })
    #     else:
    #         search_body['query']['bool']['must'].append({'match_all': {}})
    #
    #     # Filters
    #     if category:
    #         search_body['query']['bool']['filter'].append({'term': {'category': category}})
    #
    #     if tags:
    #         search_body['query']['bool']['filter'].append({'terms': {'tags': tags}})
    #
    #     if author:
    #         search_body['query']['bool']['filter'].append({'term': {'author': author}})
    #
    #     if status:
    #         search_body['query']['bool']['filter'].append({'term': {'status': status}})
    #
    #     try:
    #         result = self.es.search(index=self.index_name, body=search_body)
    #
    #         documents = [
    #             DocumentResponse(id=hit['_id'], **hit['_source'])
    #             for hit in result['hits']['hits']
    #         ]
    #
    #         return SearchResponse(
    #             total_hits=result['hits']['total']['value'],
    #             max_score=result['hits']['max_score'],
    #             took_ms=result['took'],
    #             documents=documents
    #         )
    #     except Exception as e:
    #         logger.error(f"Search failed: {e}")
    #         raise
    #
    # async def bulk_create_documents(self, documents: List[DocumentCreate]) -> Dict[str, Any]:
    #     """Bulk create documents"""
    #     actions = []
    #     now = datetime.utcnow()
    #
    #     for doc in documents:
    #         doc_id = str(uuid.uuid4())
    #         doc_data = doc.dict()
    #         doc_data.update({
    #             'created_at': now,
    #             'updated_at': now
    #         })
    #
    #         actions.extend([
    #             {'index': {'_index': self.index_name, '_id': doc_id}},
    #             doc_data
    #         ])
    #
    #     try:
    #         result = self.es.bulk(body=actions)
    #         self.es.indices.refresh(index=self.index_name)
    #
    #         success_count = sum(1 for item in result['items'] if 'error' not in item.get('index', {}))
    #         error_count = len(result['items']) - success_count
    #         errors = [
    #             str(item.get('index', {}).get('error', ''))
    #             for item in result['items']
    #             if 'error' in item.get('index', {})
    #         ]
    #
    #         return {
    #             'success_count': success_count,
    #             'error_count': error_count,
    #             'errors': errors
    #         }
    #     except Exception as e:
    #         logger.error(f"Bulk create failed: {e}")
    #         raise