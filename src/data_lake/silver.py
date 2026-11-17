import pandas as pd
import logging

from minio import Minio
from io import BytesIO

from data_lake.bucket import get_data, define_object_name, send_object_to_bucket

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def cleanse_data(data: pd.DataFrame) -> None:
    logger.info("\nRealizando limpeza de dados...")
    data = data.drop_duplicates(subset=["id_venda"])
    data = data.dropna(how="any")
    data = data[(data["quantidade"] > 0) & (data["preco_unitario"] >= 0)]
    data["data_venda"] = pd.to_datetime(data["data_venda"], format="%Y-%m-%d")
    data["id_venda"] = data["id_venda"].astype("int64")
    data["quantidade"] = data["quantidade"].astype("int64")
    data["preco_unitario"] = data["preco_unitario"].astype("float64")
    data["valor_total"] = data["quantidade"] * data["preco_unitario"]
    logger.info("\nLimpeza de dados realizada.")
    return data


def manipulate_silver_layer(client: Minio, bucket_name: str, object_name: str) -> str:
    logger.info("\nIniciando manipulação de camada Silver...")
    silver_buffer = BytesIO()
    bronze_data = get_data(client, bucket_name, object_name, from_bronze=True)
    silver_data = cleanse_data(bronze_data)
    silver_data.to_parquet(silver_buffer, index=False, engine="pyarrow")
    silver_buffer.seek(0)
    silver_object_name = define_object_name(to_silver=True)
    send_object_to_bucket(
        client, bucket_name, silver_object_name, silver_buffer, to_silver=True
    )
    logger.info("\nManipulação de camada Silver realizada.")
    return silver_object_name
