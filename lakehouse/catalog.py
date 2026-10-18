"""Open the Iceberg catalog.

The default is the REST catalog from docker-compose. The REST server advertises
MinIO as `http://minio:9000`, which only resolves inside the compose network, so
the S3 endpoint and credentials are also set here, from the host's point of view.
A `sqlite:///path` URI selects a local SQL catalog with a filesystem warehouse
instead, which the tests use and which needs no containers.
"""

from pathlib import Path
from typing import Optional

from pyiceberg.catalog import Catalog, load_catalog

from lakehouse import config


def get_catalog(uri: Optional[str] = None, warehouse: Optional[str] = None) -> Catalog:
    uri = uri or config.CATALOG_URI
    if uri.startswith("sqlite:"):
        warehouse = warehouse or str(Path(uri.removeprefix("sqlite:///")).parent / "warehouse")
        Path(warehouse).mkdir(parents=True, exist_ok=True)
        return load_catalog("lakehouse", type="sql", uri=uri, warehouse=f"file://{warehouse}")
    return load_catalog(
        "lakehouse",
        type="rest",
        uri=uri,
        **{
            "s3.endpoint": config.S3_ENDPOINT,
            "s3.access-key-id": config.S3_ACCESS_KEY,
            "s3.secret-access-key": config.S3_SECRET_KEY,
            "s3.region": config.S3_REGION,
            "s3.path-style-access": "true",
        },
    )
