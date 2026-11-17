from minio import Minio
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO

import os
import json
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

env_path = Path(__file__).parents[2].resolve() / ".env"
load_dotenv(env_path)

endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")
bucket_name = "data-lake"


def define_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    logger.info("\nDefinindo cliente para manipulação de Data Lake...")
    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    logger.info("\nCliente definido.")
    return client


def create_bucket(client: Minio, bucket: str) -> None:
    logger.info(f'\nCriando bucket "{bucket}"...')
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    logger.info("\nBucket criado.")


def define_object_name(
    to_bronze: bool = None, to_silver: bool = None, to_gold: bool = None
) -> str:
    logger.info("\nDefinindo nome de objeto para enviá-lo ao bucket...")
    timestamp = datetime.now().strftime(format="%Y%m%d_%H%M%S")
    if to_bronze:
        object_name = f"bronze/vendas/ingestion_date={timestamp}/vendas.json"
    if to_silver:
        object_name = f"silver/vendas/processing_date={timestamp}/vendas.parquet"
    if to_gold:
        object_name = (
            f"gold/indicadores/processing_date={timestamp}/vendas_por_produto.parquet"
        )
    logger.info(f"\nObjeto nomeado: {object_name}.")
    return object_name


def send_object_to_bucket(
    client: Minio,
    bucket_name: str,
    object_name: str,
    data: BytesIO,
    to_bronze: bool = None,
    to_silver: bool = None,
    to_gold: bool = None,
) -> None:
    logger.info(f'\nEnviando objeto "{object_name}" ao bucket "{bucket_name}"...')
    if to_bronze:
        content_type = "application/json"
    if to_silver or to_gold:
        content_type = "application/octet-stream"
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data,
        length=data.getbuffer().nbytes,
        content_type=content_type,
    )
    logger.info("\nObjeto enviado.")


def get_data(
    client: Minio,
    bucket_name: str,
    object_name: str,
    from_bronze: bool = None,
    from_silver: bool = None,
    from_gold: bool = None,
) -> pd.DataFrame:
    logger.info(f'\nRecuperando objeto "{object_name}" do bucket...')
    response = client.get_object(bucket_name=bucket_name, object_name=object_name)
    if from_bronze:
        data = json.loads(response.read())
        df = pd.DataFrame(data)
    if from_silver or from_gold:
        df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    logger.info("\nObjeto recuperado.")
    return df

def list_itens(client: Minio, bucket_name: str) -> None:
    objects = client.list_objects(
        bucket_name=bucket_name,
        recursive=True
    )
    print('Objetos no Data Lake: ')
    for obj in objects:
        print(f'Objeto: {obj.object_name} | Tamanho: {obj.size}')