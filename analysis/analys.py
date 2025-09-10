import config
from utilities.elasticsearch.elasticSearch_repository import ElasticSearchRepository
from utilities.elasticsearch.elasticsearch_service import ElasticsearchService
from utilities.logger import Logger

logger = Logger.get_logger()


class Analysis:
    def __init__(
        self, es_service: ElasticsearchService, hostile_words, less_hostile_words
    ):
        self.hostile_words = set(hostile_words)
        self.less_hostile_words = set(less_hostile_words)
        self.es_service = es_service
        self.es_repository = ElasticSearchRepository(self.es_service)

    def _analyze_text(self, text):
        hostile_count = (
            self._find_sub_list(text, self.hostile_words)
            * config.ANALYZER_DANGEROUS_LIST_SCORE
        )
        logger.debug(f"hostile_count: {hostile_count}")
        less_hostile_count = (
            self._find_sub_list(text, self.less_hostile_words)
            * config.ANALYZER_LOW_DANGEROUS_LIST_SCORE
        )
        logger.debug(f"less_hostile_count: {less_hostile_count}")
        scored_words = hostile_count + less_hostile_count
        logger.debug(f"scored_words: {scored_words}")
        length = len(text.split())
        logger.debug(f"length: {length}")
        danger_scored = self._percentage_calculation(scored_words, length)
        logger.debug(f"danger_scored: {danger_scored}")
        result = {
            "is_bds": self._is_it_bds(danger_scored),
            "bds_percent": danger_scored,
            "bds_threat_level": self._risk_level_calculation(danger_scored),
        }
        logger.debug(f"result: {result}")
        return result

    @staticmethod
    def _percentage_calculation(score, length):
        return (score / length) * 100

    @staticmethod
    def _is_it_bds(score):
        return score > config.ANALYZER_SCORING_TO_DETERMINE_RISK

    @staticmethod
    def _risk_level_calculation(score):
        if score < config.ANALYZER_NO_RISK_SCORE:
            return "none"
        elif score < config.ANALYZER_MEDIUM_RISK_SCORE:
            return "medium"
        else:
            return "high"

    async def run_analysis(self):
        if not await self.es_service.is_index_exists():
            logger.info("Index does not exist.")
            return None
        logger.info("Starting analysis")
        result = await self.es_repository.generic_enrich_documents(
            field_to_process="full_text",
            fields_to_include=["full_text"],
            analyzer_func=self._analyze_text,
            search_params={
                "not_exists_filters": ["is_bds", "bds_percent", "bsd_threat_level"],
                "exists_filters": ["full_text"],
            },
            process_name="danger enrichment",
        )
        logger.info("Analysis completed")
        logger.debug(f"result: {result}")
        return result

    @staticmethod
    def _find_sub_list(text:str , lst):
        count = 0
        for word in lst:
            count += text.count(word)
        return count