import os
#
# KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "podcasts")
# KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "podcasts")
# KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "default_group")
#
#
# MONGO_ATLAS_URI = os.getenv("MONGO_ATLAS_URI", "")
# MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
# MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
# MONGO_USER = os.getenv("MONGO_USER", "admin")
# MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "admin123456")
# MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "podcasts")
# MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "podcasts")
#
#
# # Build MongoDB URI
# if MONGO_ATLAS_URI:
#     MONGO_URI = MONGO_ATLAS_URI
# elif MONGO_USER and MONGO_PASSWORD:
#     MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
# else:
#     MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
#
#
# ELASTICSEARCH_PROTOCOL = os.getenv("ELASTICSEARCH_PROTOCOL", "http")
# ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "localhost")
# ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))
# ELASTICSEARCH_INDEX_DATA = os.getenv("ELASTICSEARCH_INDEX_DATA", "podcasts")
# ELASTICSEARCH_INDEX_LOG = os.getenv("ELASTICSEARCH_INDEX_LOG", "podcasts_log")
# # Data Loading Configuration
# DEFAULT_MAX_DOCUMENTS = int(os.getenv("DEFAULT_MAX_DOCUMENTS", 1000))
# DEFAULT_SUBSET = os.getenv("DEFAULT_SUBSET", "train")
#
# # API Configuration
# MAX_BULK_SIZE = int(os.getenv("MAX_BULK_SIZE", 1000))
# DEFAULT_SEARCH_LIMIT = int(os.getenv("DEFAULT_SEARCH_LIMIT", 10))
# MAX_SEARCH_LIMIT = int(os.getenv("MAX_SEARCH_LIMIT", 100))


# ----------------------------------------------
# transparency_recording

TR_KAFKA_HOST = os.getenv("TR_KAFKA_HOST", "localhost")
TR_KAFKA_PORT = int(os.getenv("TR_KAFKA_PORT", 9092))
TR_KAFKA_TOPIC_IN = os.getenv("TR_KAFKA_TOPIC_IN", "Transcription_file")
TR_KAFKA_TOPIC_OUT = os.getenv("TR_KAFKA_TOPIC_OUT", "Transcription")
TR_KAFKA_GROUP_ID = os.getenv("TR_KAFKA_GROUP_ID", "Transcription_group")



# -----------------------------------------------
# dal

DAL_KAFKA_HOST = os.getenv("DAL_KAFKA_HOST", "localhost")
DAL_KAFKA_PORT = int(os.getenv("DAL_KAFKA_PORT", 9092))
DAL_KAFKA_TOPIC_OUT = os.getenv("DAL_KAFKA_TOPIC_OUT", "podcasts_log")
DAL_KAFKA_GROUP_ID = os.getenv("DAL_KAFKA_GROUP_ID", "DAL_group")
DAL_DIRECTORY_PATH = os.getenv("DAL_DIRECTORY_PATH", "C:\podcasts")


# -------------------------------------------------
# preprocessor

PREPROCESSOR_KAFKA_HOST = os.getenv("PREPROCESSOR_KAFKA_HOST", "localhost")
PREPROCESSOR_KAFKA_PORT = int(os.getenv("PREPROCESSOR_KAFKA_PORT", 9092))
PREPROCESSOR_KAFKA_TOPIC_IN = os.getenv("PREPROCESSOR_KAFKA_TOPIC_IN", "podcasts_log")
PREPROCESSOR_KAFKA_GROUP_ID = os.getenv("PREPROCESSOR_KAFKA_GROUP_ID", "PREPROCESSOR_group")

PREPROCESSOR_KAFKA_TOPIC_OUT_TO_TRANSCRIPTION = os.getenv("PREPROCESSOR_KAFKA_TOPIC_OUT_TO_TRANSCRIPTION", "Transcription_file")
PREPROCESSOR_KAFKA_TOPIC_OUT_TO_STORAGE = os.getenv("PREPROCESSOR_KAFKA_TOPIC_OUT_TO_STORAGE", "to_storage")
PREPROCESSOR_KAFKA_TOPIC_OUT_TO_INDEX = os.getenv("PREPROCESSOR_KAFKA_TOPIC_OUT_TO_INDEX", "to_index")
