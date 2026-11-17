import logging

from io import BytesIO
from minio import Minio
import json

from data_lake.bucket import define_object_name, send_object_to_bucket

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

dados_vendas = [
    {
        "id_venda": 1,
        "data_venda": "2026-11-01",
        "cliente": "Ana",
        "produto": "Notebook",
        "quantidade": 1,
        "preco_unitario": 3500.00,
    },
    {
        "id_venda": 2,
        "data_venda": "2026-11-01",
        "cliente": "Carlos",
        "produto": "Mouse",
        "quantidade": 2,
        "preco_unitario": 80.00,
    },
    {
        "id_venda": 3,
        "data_venda": "2026-11-02",
        "cliente": "Maria",
        "produto": "Teclado",
        "quantidade": 1,
        "preco_unitario": 150.00,
    },
    {
        "id_venda": 4,
        "data_venda": "2026-11-02",
        "cliente": "João",
        "produto": "Monitor",
        "quantidade": 1,
        "preco_unitario": 1200.00,
    },
    {
        "id_venda": 5,
        "data_venda": "2026-11-03",
        "cliente": "Ana",
        "produto": "Mouse",
        "quantidade": 1,
        "preco_unitario": 80.00,
    },
    {
        "id_venda": 6,
        "data_venda": "2026-11-03",
        "cliente": "Pedro",
        "produto": "Notebook",
        "quantidade": 1,
        "preco_unitario": 3500.00,
    },
    {
        "id_venda": 6,
        "data_venda": "2026-11-03",
        "cliente": "Pedro",
        "produto": "Notebook",
        "quantidade": 1,
        "preco_unitario": 3500.00,
    },
    {
        "id_venda": 7,
        "data_venda": "2026-11-03",
        "cliente": "Lucas",
        "produto": "Teclado",
        "quantidade": -2,
        "preco_unitario": 150.00,
    },
]


def manipulate_bronze_layer(data: list[dict], client: Minio, bucket_name: str) -> str:
    logger.info("\nIniciando manipulação de camada Bronze...")
    content = json.dumps(data, ensure_ascii=False, indent=4).encode("utf-8")
    bronze_buffer = BytesIO(content)
    bronze_object_name = define_object_name(to_bronze=True)
    send_object_to_bucket(
        client, bucket_name, bronze_object_name, bronze_buffer, to_bronze=True
    )
    logger.info("\nManipulação de camada Bronze realizada.")
    return bronze_object_name
