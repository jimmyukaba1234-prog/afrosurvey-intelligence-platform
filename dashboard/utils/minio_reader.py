import os
import tempfile

import pandas as pd
from minio import Minio


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "afrosurvey-gold")


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def read_gold_table(table_name: str) -> pd.DataFrame:
    """
    Read a Gold table from MinIO and return it as a Pandas DataFrame.
    """

    client = get_minio_client()
    prefix = f"{table_name}/"

    objects = client.list_objects(
        MINIO_BUCKET,
        prefix=prefix,
        recursive=True,
    )

    parquet_files = [
        obj.object_name
        for obj in objects
        if obj.object_name.endswith(".parquet")
    ]

    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet files found for Gold table: {table_name}"
        )

    dfs = []

    with tempfile.TemporaryDirectory() as temp_dir:
        for object_name in parquet_files:
            local_file = os.path.join(
                temp_dir,
                os.path.basename(object_name),
            )

            client.fget_object(
                MINIO_BUCKET,
                object_name,
                local_file,
            )

            dfs.append(pd.read_parquet(local_file))

    return pd.concat(dfs, ignore_index=True)