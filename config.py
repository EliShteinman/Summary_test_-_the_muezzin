import os

# ----------------------------------------------
# transparency_recording
## Kafka Configuration
TR_KAFKA_HOST = os.getenv("TR_KAFKA_HOST", "localhost")
TR_KAFKA_PORT = int(os.getenv("TR_KAFKA_PORT", 9092))
TR_KAFKA_TOPIC_IN = os.getenv("TR_KAFKA_TOPIC_IN", "Transcription_file")
TR_KAFKA_TOPIC_OUT = os.getenv("TR_KAFKA_TOPIC_OUT", "to_analyzer")
TR_KAFKA_GROUP_ID = os.getenv("TR_KAFKA_GROUP_ID", "Transcription_group")

TR_MODEL_NAME = os.getenv("TR_MODEL_NAME", "tiny")
TR_DOWNLOAD_ROOT = os.getenv("TR_DOWNLOAD_ROOT", "C:\models\whisper")


# -----------------------------------------------
# dal
## Kafka Configuration
DAL_KAFKA_HOST = os.getenv("DAL_KAFKA_HOST", "localhost")
DAL_KAFKA_PORT = int(os.getenv("DAL_KAFKA_PORT", 9092))
DAL_KAFKA_TOPIC_OUT = os.getenv("DAL_KAFKA_TOPIC_OUT", "podcasts_log")
DAL_KAFKA_GROUP_ID = os.getenv("DAL_KAFKA_GROUP_ID", "DAL_group")
DAL_DIRECTORY_PATH = os.getenv("DAL_DIRECTORY_PATH", "C:\podcasts")


# -------------------------------------------------
# preprocessor
## Kafka Configuration
PREPROCESSOR_KAFKA_HOST = os.getenv("PREPROCESSOR_KAFKA_HOST", "localhost")
PREPROCESSOR_KAFKA_PORT = int(os.getenv("PREPROCESSOR_KAFKA_PORT", 9092))
PREPROCESSOR_KAFKA_TOPIC_IN = os.getenv("PREPROCESSOR_KAFKA_TOPIC_IN", "podcasts_log")
PREPROCESSOR_KAFKA_GROUP_ID = os.getenv(
    "PREPROCESSOR_KAFKA_GROUP_ID", "PREPROCESSOR_group"
)

PREPROCESSOR_KAFKA_TOPIC_OUT_TO_TRANSCRIPTION = os.getenv(
    "PREPROCESSOR_KAFKA_TOPIC_OUT_TO_TRANSCRIPTION", "Transcription_file"
)
PREPROCESSOR_KAFKA_TOPIC_OUT_TO_STORAGE = os.getenv(
    "PREPROCESSOR_KAFKA_TOPIC_OUT_TO_STORAGE", "to_storage"
)
PREPROCESSOR_KAFKA_TOPIC_OUT_TO_INDEX = os.getenv(
    "PREPROCESSOR_KAFKA_TOPIC_OUT_TO_INDEX", "to_index"
)

# -------------------------------------------------------
# storage
## Kafka Configuration
STORAGE_KAFKA_HOST = os.getenv("STORAGE_KAFKA_HOST", "localhost")
STORAGE_KAFKA_PORT = int(os.getenv("STORAGE_KAFKA_PORT", 9092))
STORAGE_KAFKA_TOPIC_IN = os.getenv("STORAGE_KAFKA_TOPIC_IN", "to_storage")
STORAGE_KAFKA_GROUP_ID = os.getenv("STORAGE_KAFKA_GROUP_ID", "storage_group")
## MongoDB Configuration
STORAGE_MONGO_ATLAS_URI = os.getenv("STORAGE_MONGO_ATLAS_URI", "")
STORAGE_MONGO_HOST = os.getenv("STORAGE_MONGO_HOST", "localhost")
STORAGE_MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
STORAGE_MONGO_USER = os.getenv("STORAGE_MONGO_PORT", "admin")
STORAGE_MONGO_PASSWORD = os.getenv("STORAGE_MONGO_PASSWORD", "admin123456")

## Build MongoDB URI
if STORAGE_MONGO_ATLAS_URI:
    STORAGE_MONGO_URI = STORAGE_MONGO_ATLAS_URI
elif STORAGE_MONGO_USER and STORAGE_MONGO_PASSWORD:
    STORAGE_MONGO_URI = f"mongodb://{STORAGE_MONGO_USER}:{STORAGE_MONGO_PASSWORD}@{STORAGE_MONGO_HOST}:{STORAGE_MONGO_PORT}/?authSource=admin"
else:
    STORAGE_MONGO_URI = f"mongodb://{STORAGE_MONGO_HOST}:{STORAGE_MONGO_PORT}/"

## mongodb collections
STORAGE_MONGO_DB_NAME = os.getenv("STORAGE_MONGO_DB_NAME", "podcasts")
STORAGE_MONGO_COLLECTION_NAME = os.getenv("STORAGE_MONGO_COLLECTION_NAME", "podcasts")


# -------------------------------------------------------
# indexer
## Kafka Configuration
INDEXER_KAFKA_HOST = os.getenv("INDEXER_KAFKA_HOST", "localhost")
INDEXER_KAFKA_PORT = int(os.getenv("INDEXER_KAFKA_PORT", 9092))
INDEXER_KAFKA_TOPIC_IN = os.getenv("INDEXER_KAFKA_TOPIC_IN", "to_index")
INDEXER_KAFKA_GROUP_ID = os.getenv("INDEXER_KAFKA_GROUP_ID", "indexer_group")

## Elasticsearch Configuration
INDEXER_ELASTICSEARCH_PROTOCOL = os.getenv("INDEXER_ELASTICSEARCH_PROTOCOL", "http")
INDEXER_ELASTICSEARCH_HOST = os.getenv("INDEXER_ELASTICSEARCH_HOST", "localhost")
INDEXER_ELASTICSEARCH_PORT = int(os.getenv("INDEXER_ELASTICSEARCH_PORT", 9200))
INDEXER_ELASTICSEARCH_INDEX_DATA = os.getenv(
    "INDEXER_ELASTICSEARCH_INDEX_DATA", "podcasts"
)
INDEXER_ELASTICSEARCH_INDEX_LOG = os.getenv(
    "INDEXER_ELASTICSEARCH_INDEX_LOG", "podcasts_log"
)

# ---------------------------------------------------------
# analyzer
ANALYZER_KAFKA_HOST = os.getenv("ANALYZER_KAFKA_HOST", "localhost")
ANALYZER_KAFKA_PORT = int(os.getenv("ANALYZER_KAFKA_PORT", 9092))
ANALYZER_KAFKA_TOPIC_IN = os.getenv("ANALYZER_KAFKA_TOPIC_IN", "to_analyzer")
ANALYZER_KAFKA_TOPIC_OUT = os.getenv("ANALYZER_KAFKA_TOPIC_IN", "to_index")
ANALYZER_KAFKA_GROUP_ID = os.getenv("ANALYZER_KAFKA_GROUP_ID", "analyzer_group")
ANALYZER_HOSTILE_WORDS = os.getenv("ANALYZER_HOSTILE_WORDS", "R2Vub2NpZGUsV2FyIENyaW1lcyxBcGFydGhlaWQsTWFzc2FjcmUsTmFrYmEsRGlzcGxhY2VtZW50LEh1bWFuaXRhcmlhbiBDcmlzaXMsQmxvY2thZGUsT2NjdXBhdGlvbixSZWZ1Z2VlcyxJQ0MsQkRT")
ANALYZER_LESS_HOSTILE_WORDS = os.getenv("ANALYZER_LESS_HOSTILE_WORDS", "RnJlZWRvbSBGbG90aWxsYSxSZXNpc3RhbmNlLExpYmVyYXRpb24sRnJlZSBQYWxlc3RpbmUsR2F6YSxDZWFzZWZpcmUsUHJvdGVzdCxVTlJXQQ==")

# ------------------------------------------------------------
# logging
LOG_ELASTICSEARCH_PROTOCOL = os.getenv("LOG_ELASTICSEARCH_PROTOCOL", "http")
LOG_ELASTICSEARCH_HOST = os.getenv("LOG_ELASTICSEARCH_HOST", "localhost")
LOG_ELASTICSEARCH_PORT = int(os.getenv("LOG_ELASTICSEARCH_PORT", 9200))
LOG_ELASTICSEARCH_INDEX_LOG = os.getenv("LOG_ELASTICSEARCH_INDEX_LOG", "logs")