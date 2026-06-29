from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, LongType,
    BooleanType, TimestampType
)
from config.app_config import AppConfig


class DataIO:
    """
    Classe responsável pela leitura e escrita de dados (I/O).

    Centraliza todas as operações de entrada e saída do pipeline,
    definindo schemas explícitos para cada fonte de dados.
    Schemas explícitos evitam a inferência automática do Spark,
    que dobra o tempo de leitura e pode interpretar tipos incorretamente.
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        """
        Inicializa o DataIO recebendo dependências via injeção.

        :param spark: SparkSession ativa
        :param config: objeto AppConfig com os caminhos dos datasets
        """
        self._spark = spark
        self._config = config

    def _schema_pedidos(self) -> StructType:
        """
        Define o schema explícito do dataset de pedidos (CSV).

        Cada StructField representa uma coluna: nome, tipo e se aceita nulo.
        O terceiro parâmetro True indica que o campo pode ser nulo,
        prevenindo erros caso existam registros incompletos nos dados.

        :return: StructType com o schema do dataset de pedidos
        """
        return StructType([
            StructField("ID_PEDIDO",      StringType(),    True),
            StructField("PRODUTO",        StringType(),    True),
            StructField("VALOR_UNITARIO", FloatType(),     True),
            StructField("QUANTIDADE",     LongType(),      True),
            StructField("DATA_CRIACAO",   TimestampType(), True),
            StructField("UF",             StringType(),    True),
            StructField("ID_CLIENTE",     LongType(),      True),
        ])

    def _schema_pagamentos(self) -> StructType:
        """
        Define o schema explícito do dataset de pagamentos (JSON).

        O campo avaliacao_fraude é um objeto aninhado no JSON,
        representado como StructType dentro de StructField.

        :return: StructType com o schema do dataset de pagamentos
        """
        return StructType([
            StructField("id_pedido",       StringType(),  True),
            StructField("forma_pagamento", StringType(),  True),
            StructField("valor_pagamento", FloatType(),   True),
            StructField("status",          BooleanType(), True),
            # Campo aninhado: objeto JSON com resultado da análise de fraude
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score",  FloatType(),   True),
            ]), True),
        ])

    def ler_pedidos(self) -> DataFrame:
        """
        Lê todos os arquivos de pedidos da pasta configurada.

        O Spark lê automaticamente todos os arquivos da pasta,
        incluindo arquivos comprimidos (.gz).

        :return: DataFrame com os dados de pedidos
        """
        return (
            self._spark.read
            .schema(self._schema_pedidos())
            .option("header", "true")   # primeira linha é o cabeçalho
            .option("sep", ";")         # separador é ponto-e-vírgula
            .csv(self._config.caminho_pedidos)
        )

    def ler_pagamentos(self) -> DataFrame:
        """
        Lê todos os arquivos de pagamentos da pasta configurada.

        :return: DataFrame com os dados de pagamentos
        """
        return (
            self._spark.read
            .schema(self._schema_pagamentos())
            .json(self._config.caminho_pagamentos)
        )

    def escrever_parquet(self, df: DataFrame) -> None:
        """
        Grava o DataFrame resultado em formato parquet.

        O modo 'overwrite' sobrescreve execuções anteriores,
        permitindo rodar o pipeline múltiplas vezes sem erro.

        :param df: DataFrame processado pronto para gravação
        """
        (
            df.write
            .mode("overwrite")
            .parquet(self._config.caminho_saida)
        )