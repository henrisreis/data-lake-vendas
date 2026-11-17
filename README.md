# Data Lake e Arquitetura Medallion | Vendas

## 1. Visão geral e arquitetura

Este projeto demonstra um pipeline batch de vendas com Arquitetura Medallion de Data Lake
em três camadas. O fluxo lê dados de exemplo mantidos em memória, grava o dado
bruto no MinIO, aplica limpeza com pandas e gera um agregado analítico com
DuckDB. Os objetos processados são armazenados em Parquet.

**Escopo observado:** não há conexão implementada com banco transacional, API,
CRM ou arquivos externos. A origem atual é a lista `dados_vendas` em
`src/data_lake/bronze.py`.

```text
Lista Python de vendas
        |
        v
Bronze: JSON bruto ---------------------> MinIO / bucket data-lake
        |                                           |
        v                                           |
Silver: pandas (limpeza e tipagem)                  |
        |                                           |
        v                                           |
Gold: DuckDB (agregação por produto)                |
        |                                           |
        +---------- Parquet ------------------------+
                                                    |
                                                    v
                                  Consulta/listagem de objetos no MinIO
```

O pipeline é iniciado por `main.py` e executa as três camadas em sequência.
Cada execução inclui um timestamp no caminho dos objetos.

| Tecnologia | Uso neste projeto |
| --- | --- |
| Python 3.10 ou superior | Implementação e execução das etapas |
| MinIO (AIStor) | Armazenamento de objetos das camadas Bronze, Silver e Gold |
| pandas | Leitura em DataFrame e limpeza da camada Silver |
| PyArrow | Serialização das camadas Silver e Gold em Parquet |
| DuckDB | Consulta SQL sobre o DataFrame Silver para produzir Gold |
| Docker Compose | Execução local do serviço MinIO |
| uv | Gerenciamento de dependências e execução do projeto |
| python-dotenv | Leitura de configurações locais em `.env` |

As tecnologias acima são as implementadas no repositório.

## 2. Fontes de dados e contratos

### Origem atual

A origem é uma lista Python de registros de venda definida em
`src/data_lake/bronze.py`. O exemplo contém oito registros, incluindo um ID
duplicado e uma quantidade negativa para exercitar a limpeza.

| Campo de origem | Tipo esperado | Descricao |
| --- | --- | --- |
| `id_venda` | Inteiro | Identificador da venda |
| `data_venda` | Texto no formato `YYYY-MM-DD` | Data da venda |
| `cliente` | Texto | Nome do cliente |
| `produto` | Texto | Produto vendido |
| `quantidade` | Numérico | Quantidade de unidades |
| `preco_unitario` | Numérico | Preço por unidade; a moeda não está especificada |

### Armazenamento

- Serviço: MinIO configurado em `docker-compose.yaml`.
- Bucket: `data-lake`, criado pelo pipeline se ainda não existir.
- Credenciais e endpoint: variaveis de ambiente `MINIO_ROOT_USER`,
  `MINIO_ROOT_PASSWORD` e `MINIO_ENDPOINT`.
- Bronze: `bronze/vendas/ingestion_date=<timestamp>/vendas.json`.
- Silver: `silver/vendas/processing_date=<timestamp>/vendas.parquet`.
- Gold: `gold/indicadores/processing_date=<timestamp>/vendas_por_produto.parquet`.

O timestamp atual é gerado na execução local no formato
`YYYYMMDD_HHMMSS`; apesar dos nomes `ingestion_date` e `processing_date`, o
valor inclui hora, minuto e segundo e não é uma data de negócio.

## 3. Pipeline e transformações (ETL)

### Bronze: ingestão

`manipulate_bronze_layer` serializa a lista de vendas como JSON UTF-8 com
indentação e envia o objeto ao bucket. A camada preserva os registros da
origem; não aplica validação nem limpeza.

### Silver: limpeza e tipagem

`manipulate_silver_layer` recupera o JSON e aplica, nesta ordem, as seguintes
regras:

1. Remove duplicatas por `id_venda`, mantendo a primeira ocorrência conforme a
   ordem de entrada.
2. Remove linhas que tenham qualquer valor nulo.
3. Mantém somente linhas com `quantidade > 0` e `preco_unitario >= 0`.
4. Converte `data_venda` para data/hora usando o formato `%Y-%m-%d`.
5. Converte `id_venda` e `quantidade` para inteiro de 64 bits, e
   `preco_unitario` para ponto flutuante de 64 bits.
6. Calcula `valor_total = quantidade * preco_unitario`.
7. Grava o resultado em Parquet.

### Gold: agregação

`manipulate_gold_layer` lê o Parquet Silver, registra o DataFrame no DuckDB com
o nome `vendas` e executa uma agregação por `produto`:

- `quantidade_vendida`: soma de `quantidade`;
- `faturamento`: soma de `valor_total`;
- `total_vendas`: contagem distinta de `id_venda`.

O resultado é gravado em Parquet.

## 4. Modelo de dados e dicionário de dados

É implementada uma Arquitetura Medallion, composta pelas camadas Bronze, Silver e Gold. 
Bronze armazena registros brutos, Silver mantém uma tabela plana limpa, e Gold apresenta uma agregação por produto. 
Os tipos abaixo descrevem os tipos definidos no código ou esperados na saída da consulta.

### Silver: `silver/vendas`

Todos os campos abaixo são obrigatórios na saída: linhas com nulos são
removidas antes da gravação.

| Coluna | Tipo | Nulidade | Descrição / regra |
| --- | --- | --- | --- |
| `id_venda` | `int64` | Não | ID da venda; duplicatas por este campo são removidas |
| `data_venda` | `datetime64[ns]` | Não | Data convertida a partir de `YYYY-MM-DD` |
| `cliente` | Texto (`object` no pandas) | Não | Cliente da origem, sem normalização |
| `produto` | Texto (`object` no pandas) | Não | Produto da origem, sem normalização |
| `quantidade` | `int64` | Não | Quantidade positiva |
| `preco_unitario` | `float64` | Não | Preço maior ou igual a zero; moeda não definida |
| `valor_total` | `float64` | Não | Produto de quantidade por preço unitário |

### Gold: `gold/indicadores/vendas_por_produto`

| Coluna | Tipo esperado | Nulidade | Descrição / regra |
| --- | --- | --- | --- |
| `produto` | Texto | Não | Chave do agrupamento |
| `quantidade_vendida` | Inteiro (tipo de retorno da agregação DuckDB) | Não | Soma das quantidades válidas para o produto |
| `faturamento` | Ponto flutuante (`DOUBLE`) | Não | Soma de `valor_total`; não inclui moeda ou arredondamento comercial |
| `total_vendas` | Inteiro | Não | Quantidade distinta de `id_venda` por produto |

## 5. Operação, monitoramento e alertas

### Pré-requisitos

- Docker com Docker Compose;
- `uv`;
- Python 3.10 ou superior;
- arquivo de licença MinIO `minio.license` no diretório raiz, conforme o
  volume definido no Compose.

### Configuração local

Crie um arquivo `.env` na raiz do repositório. Não versione credenciais reais.
Para executar o pipeline diretamente na máquina host, configure:

```dotenv
MINIO_ENDPOINT=localhost:9000
MINIO_ROOT_USER=<usuário-local>
MINIO_ROOT_PASSWORD=<senha-local>
```

O Compose usa as duas credenciais para iniciar o MinIO, e o pipeline usa as
mesmas credenciais junto com `MINIO_ENDPOINT` para conectar ao serviço. O
endpoint `localhost:9000` é adequado para o processo executado no host; um
processo executado em outro container da rede Compose normalmente usaria
`minio:9000`.

### Execução manual

Na raiz do projeto:

```bash
docker compose up -d
docker compose ps
uv sync
uv run python main.py
```

O programa cria o bucket se necessário, executa Bronze, Silver e Gold e lista
os objetos encontrados. A interface MinIO fica disponível em
`http://localhost:9001`, usando as credenciais locais configuradas. Para
acompanhar o serviço:

```bash
docker compose logs -f minio
```

Para parar o serviço sem remover os dados persistidos no diretório local
`data/`:

```bash
docker compose down
```

O pipeline escreve logs INFO das etapas e imprime a listagem final de objetos.
Falhas de conexão, parsing ou consulta interrompem a execução.