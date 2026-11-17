import sys
import os

from pathlib import Path
from dotenv import load_dotenv

PROJECT_DIR = Path.cwd()
sys.path.append(str(PROJECT_DIR / "src"))

from data_lake.bronze import dados_vendas

from data_lake.bucket import define_client, create_bucket, list_itens
from data_lake.bronze import manipulate_bronze_layer
from data_lake.silver import manipulate_silver_layer
from data_lake.gold import manipulate_gold_layer

env_path = PROJECT_DIR / ".env"
load_dotenv(env_path)

endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")
bucket_name = "data-lake"
data = dados_vendas

# Bucket
client = define_client(endpoint, access_key, secret_key)
create_bucket(client, bucket_name)

# Bronze Layer
bronze_object_name = manipulate_bronze_layer(data, client, bucket_name)

# Silver Layer
silver_object_name = manipulate_silver_layer(client, bucket_name, bronze_object_name)

# Gold Layer
table_name = "vendas"
query = f"""
SELECT
    produto,
    SUM(quantidade) AS quantidade_vendida,
    SUM(valor_total) AS faturamento,
    COUNT(DISTINCT id_venda) AS total_vendas
FROM {table_name}
GROUP BY produto
ORDER BY faturamento DESC
"""
gold_object_name = manipulate_gold_layer(
    client, bucket_name, silver_object_name, query, table_name
)

# List objects
list_itens(client, bucket_name)
