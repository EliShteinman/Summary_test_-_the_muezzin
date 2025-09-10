FROM python:3.13

RUN apt-get update
RUN apt-get install -y ffmpeg
RUN pip install --upgrade pip
RUN pip install -U openai-whisper
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TR_MODEL_NAME="tiny" \
    TR_DOWNLOAD_ROOT=/app/models

WORKDIR /app
COPY ./transparency_recording/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN mkdir /app/data
RUN mkdir /app/models
RUN mkdir /app/tts
COPY ./utilities /app/utilities
COPY ./transparency_recording /app/transparency_recording
COPY ./config.py /app/config.py
CMD ["python3", "/app/transparency_recording/main.py"]