# Data Engineering Programming — Projeto Final

**Disciplina:** Data Engineering Programming  
**MBA:** Engenharia de Dados — FIAP  

## Descrição

Pipeline PySpark que gera um relatório de pedidos de venda com
pagamentos recusados (status=false) classificados como legítimos
na avaliação de fraude (fraude=false), referentes ao ano de 2025.

## Pré-requisitos

- Python 3.12+
- Java 17+
- Windows: `JAVA_HOME` e `HADOOP_HOME` configurados

## Instalação

Clone os datasets necessários dentro da pasta `data/`:

```bash
cd data
git clone https://github.com/infobarbosa/datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos
cd ..
```

Crie e ative o ambiente virtual:

```bash
python -m venv venv

# Windows
.\venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

## Execução

```bash
python main.py
```

O relatório será gravado em formato parquet na pasta `output/relatorio_pedidos/`.

## Testes

```bash
pytest tests/ -v
```

## Estrutura do projeto

```
├── config/          # Configurações centralizadas
├── session/         # Gerenciamento da SparkSession
├── io_utils/        # Leitura e escrita de dados
├── business/        # Lógica de negócio
├── pipeline/        # Orquestração do pipeline
├── tests/           # Testes unitários
├── data/            # Datasets de entrada
├── output/          # Relatório gerado
└── main.py          # Ponto de entrada
```