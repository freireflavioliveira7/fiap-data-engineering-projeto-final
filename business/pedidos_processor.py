import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from config.app_config import AppConfig

# Configuração do logger no nível de módulo.
# format define o padrão de cada linha de log:
# data/hora - nível (INFO, ERROR, etc) - mensagem
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class PedidosProcessor:
    """
    Classe responsável pela lógica de negócio do pipeline.

    Aplica as regras definidas pelo enunciado:
    - Pagamentos recusados (status=False)
    - Classificados como legítimos na avaliação de fraude (fraude=False)
    - Apenas pedidos do ano configurado em AppConfig (2025)

    O resultado é ordenado por UF, forma de pagamento e data do pedido.
    """

    def __init__(self, config: AppConfig):
        """
        Inicializa o processador recebendo configurações via injeção de dependência.

        :param config: objeto AppConfig com parâmetros da aplicação
        """
        self._config = config
        # Cria um logger nomeado com o nome da própria classe.
        # Facilita identificar a origem das mensagens nos logs.
        self._logger = logging.getLogger(self.__class__.__name__)

    def processar(self, df_pedidos: DataFrame, df_pagamentos: DataFrame) -> DataFrame:
        """
        Executa o processamento completo dos dados.

        Etapas:
        1. Filtra pagamentos recusados e legítimos
        2. Realiza JOIN entre pedidos e pagamentos
        3. Filtra pelo ano de referência
        4. Calcula valor total e seleciona colunas do relatório
        5. Ordena o resultado

        :param df_pedidos: DataFrame com os dados de pedidos
        :param df_pagamentos: DataFrame com os dados de pagamentos
        :return: DataFrame processado e ordenado
        """
        try:
            self._logger.info("Iniciando processamento dos pedidos...")

            self._logger.info("Filtrando pagamentos recusados e legitimos...")
            df_pagamentos_filtrados = df_pagamentos.filter(
                # status=False: pagamento recusado pelo operador financeiro
                # fraude=False: avaliação de fraude classificou como legítimo
                # O operador & exige parênteses em cada condição no PySpark
                (F.col("status") == False) &
                (F.col("avaliacao_fraude.fraude") == False)
            )

            self._logger.info("Realizando join entre pedidos e pagamentos...")
            df_joined = df_pedidos.join(
                df_pagamentos_filtrados,
                # Chave de junção: ID_PEDIDO nos pedidos = id_pedido nos pagamentos
                df_pedidos["ID_PEDIDO"] == df_pagamentos_filtrados["id_pedido"],
                # INNER JOIN: mantém apenas registros presentes nos dois datasets
                "inner"
            )

            self._logger.info("Filtrando pedidos do ano de 2025...")
            df_2025 = df_joined.filter(
                # Extrai o ano da coluna de data e compara com o ano configurado
                # Usar self._config.ano_filtro evita valor hardcoded (2025)
                F.year(F.col("DATA_CRIACAO")) == self._config.ano_filtro
            )

            self._logger.info("Calculando valor total e selecionando colunas...")
            df_resultado = df_2025.select(
                df_pedidos["ID_PEDIDO"].alias("id_pedido"),
                F.col("UF").alias("uf"),
                F.col("forma_pagamento"),
                # valor_total = valor unitário × quantidade do produto no pedido
                (F.col("VALOR_UNITARIO") * F.col("QUANTIDADE")).alias("valor_total"),
                F.col("DATA_CRIACAO").alias("data_pedido")
            ).orderBy(
                "uf",             # 1º critério de ordenação
                "forma_pagamento", # 2º critério de ordenação
                "data_pedido"      # 3º critério de ordenação
            )

            self._logger.info("Processamento concluido com sucesso!")
            return df_resultado

        except Exception as e:
            # Registra o erro com nível ERROR antes de relançar a exceção.
            # O raise garante que o erro não seja silenciado — quem chamou
            # este método também será notificado da falha.
            self._logger.error(f"Erro durante o processamento: {e}")
            raise