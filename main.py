import os
import sys

# Garante que o PySpark use o mesmo Python que está executando o projeto.
# Necessário no Windows onde o executável se chama 'python' e não 'python3'.
# sys.executable retorna o caminho completo do Python atual — solução
# agnóstica à plataforma, funciona em Windows, Linux e Mac.
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from config.app_config import AppConfig
from session.spark_manager import SparkSessionManager
from io_utils.data_io import DataIO
from business.pedidos_processor import PedidosProcessor
from pipeline.orchestrator import PipelineOrchestrator


def main():
    """
    Ponto de entrada e Aggregation Root da aplicação.

    Responsável por instanciar todas as dependências e injetá-las
    nas classes que precisam delas. Nenhuma classe cria suas próprias
    dependências — todas recebem objetos prontos via este método.

    Ordem de instanciação segue a hierarquia de dependências:
    config → spark → data_io / processor → pipeline
    """

    # 1. Configurações — base de todas as outras dependências
    config = AppConfig()

    # 2. Sessão Spark — depende do config para o nome da aplicação
    spark_manager = SparkSessionManager(config)
    spark = spark_manager.get_session()

    # 3. I/O e processador — dependem do spark e/ou config
    data_io = DataIO(spark, config)
    processor = PedidosProcessor(config)

    # 4. Pipeline — depende do data_io e do processor
    pipeline = PipelineOrchestrator(data_io, processor)

    # 5. Execução — dispara leitura, processamento e escrita
    pipeline.executar()

    # 6. Encerramento — libera recursos alocados pelo Spark
    spark_manager.stop_session()


# Garante que main() só execute quando o arquivo é chamado diretamente.
# Se outro arquivo importar este módulo, main() não será executado
# automaticamente — evita efeitos colaterais indesejados em testes.
if __name__ == "__main__":
    main()