import duckdb
import logging
import pandas as pd

from io import BytesIO
from minio import Minio

from data_lake.bucket import get_data, define_object_name, send_object_to_bucket

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def query_data(data: pd.DataFrame, query: str, table_name: str) -> pd.DataFrame:
    logger.info("\nConsultando dados...")
    con = duckdb.connect()
    con.register(table_name, data)
    df = con.execute(query).df()
    con.close()
    logger.info("\nConsulta realizada.")
    return df


def manipulate_gold_layer(
    client: Minio, bucket_name: str, object_name: str, query: str, table_name: str
):
    logger.info("\nIniciando manipulação de camada Gold...")
    gold_buffer = BytesIO()
    silver_data = get_data(client, bucket_name, object_name, from_silver=True)
    gold_data = query_data(silver_data, query, table_name)
    gold_data.to_parquet(gold_buffer, index=False, engine="pyarrow")
    gold_buffer.seek(0)
    gold_object_name = define_object_name(to_gold=True)
    send_object_to_bucket(
        client, bucket_name, gold_object_name, gold_buffer, to_gold=True
    )
    logger.info("\nManipulação de camada Gold realizada.")
    return gold_object_name
