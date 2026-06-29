from io_utils.data_io import DataIO
from business.pedidos_processor import PedidosProcessor


class PipelineOrchestrator:
    """
    Classe responsável pela orquestração do pipeline de dados.

    Coordena a sequência de execução das etapas sem implementar
    nenhuma lógica de negócio ou I/O diretamente — delega cada
    responsabilidade para a classe especializada correspondente.

    Essa separação garante que mudanças na lógica de negócio ou
    na forma de ler/escrever dados não impactem a orquestração.
    """

    def __init__(self, data_io: DataIO, processor: PedidosProcessor):
        """
        Inicializa o orquestrador recebendo dependências via injeção.

        :param data_io: objeto responsável pela leitura e escrita de dados
        :param processor: objeto responsável pela lógica de negócio
        """
        self._data_io = data_io
        self._processor = processor

    def executar(self):
        """
        Executa o pipeline completo em três etapas sequenciais:

        1. Leitura — carrega os datasets de pedidos e pagamentos
        2. Processamento — aplica filtros, join, cálculos e ordenação
        3. Escrita — grava o resultado em formato parquet
        """
        # Etapa 1: leitura dos dados de entrada
        df_pedidos = self._data_io.ler_pedidos()
        df_pagamentos = self._data_io.ler_pagamentos()

        # Etapa 2: processamento com as regras de negócio
        df_resultado = self._processor.processar(df_pedidos, df_pagamentos)

        # Etapa 3: escrita do relatório final em parquet
        self._data_io.escrever_parquet(df_resultado)