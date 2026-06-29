class AppConfig:
    """
    Classe de configuração centralizada do projeto.
    
    Concentra todos os parâmetros necessários para execução do pipeline,
    evitando valores espalhados pelo código (hardcoded values).
    """

    def __init__(self):
        # Nome da aplicação exibido na Spark UI e nos logs
        self.app_name = "Relatorio Pedidos Recusados Legitimos"

        # Caminhos dos datasets de entrada (relativos à raiz do projeto)
        self.caminho_pedidos = "data/datasets-csv-pedidos/data/pedidos"
        self.caminho_pagamentos = "data/dataset-json-pagamentos/data/pagamentos"

        # Caminho de saída do relatório final em formato parquet
        self.caminho_saida = "output/relatorio_pedidos"

        # Ano de referência para filtro dos pedidos
        self.ano_filtro = 2025

        # Formato de escrita do relatório
        self.formato_saida = "parquet"