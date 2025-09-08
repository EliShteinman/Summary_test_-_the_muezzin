import asyncio
import logging
import uvicorn
from pathlib import Path
from data_load import load_meta_data_for_directory, load_meta_data_for_file
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
import config
from utilities.kafka.async_client import KafkaProducerAsync
logging.basicConfig(level=config.LOG_LEVEL)
logging.getLogger("kafka").setLevel(level=config.LOG_KAFKA)
logger = logging.getLogger(__name__)

producer: KafkaProducerAsync | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logger.info("Starting retriever service...")
    boostrap_servers = f"{config.KAFKA_URL}:{config.KAFKA_PORT}"
    producer = KafkaProducerAsync(
        bootstrap_servers=boostrap_servers
    )
    logger.info(
        f"Initializing Kafka producer - {producer.get_config()}"
    )

    try:
        await producer.start()
        logger.info("Kafka producer started successfully")
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start Kafka producer: {e}", )

    logger.info("Starting main processing loop...")

    yield

    logger.info("Application shutdown...")
    try:
        pass
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

app = FastAPI(
    lifespan=lifespan,
    title="Podcast Retriever API",
    version="1.0",
    description="API for retrieving podcasts",
)


@app.get("/")
async def root():
    return {"message": "Podcast Retriever API"}


@app.get("/load_file/{file_path}")
async def load_file(file_path: Path):
    logger.info(f"Loading file: {file_path}")
    try:
        logger.info(Path(file_path))
        meta_data = load_meta_data_for_file(file_path)
        await producer.send_message(
            config.KAFKA_OUTPUT_TOPIC, meta_data
        )
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        return {"status": "error"}



@app.get("/load_directory/{directory_path}")
async def load_directory(directory_path: Path):
    logger.info(f"Loading directory: {directory_path}")
    try:
        for meta_data in load_meta_data_for_directory(directory_path):
            await producer.send_message(
                config.KAFKA_OUTPUT_TOPIC, meta_data
            )
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        return {"status": "error"}

if __name__ == "__main__":
    uvicorn.run(app=app, host="0.0.0.0", port=8000)
