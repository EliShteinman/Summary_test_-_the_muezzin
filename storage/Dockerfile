FROM python:3.13

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR app
COPY ./storage/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN mkdir /app/data
COPY ./utilities /app/utilities
COPY ./storage /app/storage
COPY ./config.py /app/config.py

CMD ["python3", "/app/storage/main.py"]