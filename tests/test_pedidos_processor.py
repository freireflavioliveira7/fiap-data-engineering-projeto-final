import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, LongType, BooleanType, TimestampType
)
from business.pedidos_processor import PedidosProcessor
from config.app_config import AppConfig


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local")
        .appName("test_pedidos_processor")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def config():
    return AppConfig()


class TestPedidosProcessor:

    def test_filtro_pagamentos_recusados_legitimos(self, spark, config):
        schema_pedidos = StructType([
            StructField("ID_PEDIDO",      StringType(),    True),
            StructField("PRODUTO",        StringType(),    True),
            StructField("VALOR_UNITARIO", FloatType(),     True),
            StructField("QUANTIDADE",     LongType(),      True),
            StructField("DATA_CRIACAO",   TimestampType(), True),
            StructField("UF",             StringType(),    True),
            StructField("ID_CLIENTE",     LongType(),      True),
        ])

        schema_pagamentos = StructType([
            StructField("id_pedido",       StringType(),  True),
            StructField("forma_pagamento", StringType(),  True),
            StructField("valor_pagamento", FloatType(),   True),
            StructField("status",          BooleanType(), True),
            StructField("avaliacao_fraude", StructType([
                StructField("fraude", BooleanType(), True),
                StructField("score",  FloatType(),   True),
            ]), True),
        ])

        pedidos_data = [
            ("id-001", "NOTEBOOK",      1500.0, 1, datetime(2025, 1, 15), "SP", 1),
            ("id-002", "TABLET",        1100.0, 2, datetime(2025, 3, 20), "RJ", 2),
            ("id-003", "CELULAR",       1000.0, 1, datetime(2024, 6, 10), "MG", 3),
        ]

        pagamentos_data = [
            Row(id_pedido="id-001", forma_pagamento="PIX",           valor_pagamento=1500.0, status=False, avaliacao_fraude=Row(fraude=False, score=0.1)),
            Row(id_pedido="id-002", forma_pagamento="BOLETO",        valor_pagamento=2200.0, status=False, avaliacao_fraude=Row(fraude=True,  score=0.95)),
            Row(id_pedido="id-003", forma_pagamento="CARTAO_CREDITO",valor_pagamento=1000.0, status=False, avaliacao_fraude=Row(fraude=False, score=0.2)),
        ]

        df_pedidos    = spark.createDataFrame(pedidos_data, schema_pedidos)
        df_pagamentos = spark.createDataFrame(pagamentos_data, schema_pagamentos)

        processor    = PedidosProcessor(config)
        df_resultado = processor.processar(df_pedidos, df_pagamentos)

        assert df_resultado.count() == 1

        row = df_resultado.collect()[0]
        assert row["id_pedido"]       == "id-001"
        assert row["uf"]              == "SP"
        assert row["forma_pagamento"] == "PIX"
        assert row["valor_total"]     == 1500.0