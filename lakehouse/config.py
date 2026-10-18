"""Connection settings, overridable by environment variables (defaults match docker-compose.yml)."""

import os

CATALOG_URI = os.getenv("LAKEHOUSE_CATALOG_URI", "http://127.0.0.1:8181")
S3_ENDPOINT = os.getenv("LAKEHOUSE_S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
S3_REGION = os.getenv("LAKEHOUSE_S3_REGION", "us-east-1")
BUCKET = os.getenv("LAKEHOUSE_BUCKET", "warehouse")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081")
