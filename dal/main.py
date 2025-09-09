from contextlib import asynccontextmanager
from pathlib import Path
from time import sleep

import uvicorn
from data_load import load_meta_data_for_directory, load_meta_data_for_file
from fastapi import FastAPI, HTTPException, status

import config
from utilities.kafka.async_client import KafkaProducerAsync
from utilities.logger import Logger

logger = Logger.get_logger()

producer: KafkaProducerAsync | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logger.info("Starting retriever service...")
    boostrap_servers = rf"{config.DAL_KAFKA_HOST}:{config.DAL_KAFKA_PORT}"
    producer = KafkaProducerAsync(
        bootstrap_servers=boostrap_servers
    )
    try:
        await producer.start()
        logger.info("Kafka producer started successfully")
        logger.info(f"Initializing Kafka producer - {producer.get_config()}")
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start Kafka producer: {e}",
        )

    logger.info("Starting main processing loop...")

    yield

    logger.info("Application shutdown...")
    try:
        producer.stop()
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
    logger.debug("health check is running")
    return {"message": "Podcast Retriever API"}


@app.get("/load_file/{file_path}")
async def load_file(file_path: Path):
    logger.info(f"Loading file: {file_path}")
    try:
        logger.debug(Path(file_path))
        meta_data = load_meta_data_for_file(file_path)
        logger.debug(meta_data)
        await producer.send_message(config.DAL_KAFKA_TOPIC_OUT, meta_data)
        return {
            "status": "success",
            "file_path": meta_data["file_path"],
            "meta_data": meta_data["meta_data"],
        }
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        return {
            "status": "error",
            "error_message": f"Error loading file: {e}",
        }


@app.get("/load_directory/{directory_path}")
async def load_directory(directory_path: Path):
    logger.info(f"Loading directory: {directory_path}")
    try:
        num_files = 0
        results = []
        for meta_data in load_meta_data_for_directory(directory_path):
            logger.debug(meta_data)
            num_files += 1
            results.append(meta_data)
            await producer.send_message(config.DAL_KAFKA_TOPIC_OUT, meta_data)
        return {
            "status": "success",
            "num_files": num_files,
            "results": results,
        }
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        return {"status": "error"}


if __name__ == "__main__":
    uvicorn.run(app=app, host="0.0.0.0", port=8000)
