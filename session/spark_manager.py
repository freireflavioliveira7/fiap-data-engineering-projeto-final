import os
import sys
from pyspark.sql import SparkSession
from config.app_config import AppConfig

# Garante que o PySpark use o mesmo Python que está executando o projeto.
# Necessário no Windows onde o executável se chama 'python' e não 'python3'.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class SparkSessionManager:
    """
    Gerenciador da SparkSession.

    Responsável por criar, fornecer e encerrar a sessão Spark.
    Utiliza o padrão lazy initialization — a sessão só é criada
    quando solicitada pela primeira vez, evitando alocação
    desnecessária de recursos.
    """

    def __init__(self, config: AppConfig):
        """
        Inicializa o gerenciador recebendo as configurações via injeção de dependência.

        :param config: objeto AppConfig com os parâmetros da aplicação
        """
        self._config = config
        self._spark = None  # sessão ainda não criada

    def get_session(self) -> SparkSession:
        """
        Retorna a SparkSession ativa, criando-a se ainda não existir.

        O uso de getOrCreate() garante que apenas uma sessão
        seja criada por processo, evitando conflitos.

        :return: SparkSession ativa
        """
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(self._config.app_name)
                .getOrCreate()
            )
        return self._spark

    def stop_session(self):
        """
        Encerra a SparkSession e libera os recursos alocados.
        Deve ser chamado ao final da execução do pipeline.
        """
        if self._spark is not None:
            self._spark.stop()
            self._spark = None