# Projeto de Engenharia de Dados com Databricks: Gastos Parlamentares da Câmara dos Deputados

Este projeto tem como objetivo construir um pipeline completo de dados utilizando Databricks, Delta Lake e arquitetura em camadas Bronze, Silver e Gold.

A base utilizada contém dados públicos da Câmara dos Deputados referentes à Cota para Exercício da Atividade Parlamentar, conhecida como CEAP. Esses dados permitem analisar despesas parlamentares, partidos, unidades federativas, tipos de despesa, fornecedores e evolução mensal dos gastos.

## Objetivo do projeto

O objetivo principal é simular um projeto real de engenharia e análise de dados, desde a ingestão de arquivos brutos até a criação de tabelas analíticas prontas para consumo em ferramentas de BI.

Com este projeto, é possível responder perguntas como:

- Quais parlamentares tiveram maior volume de gastos?
- Quais partidos concentraram mais despesas?
- Quais estados tiveram maior valor gasto?
- Quais tipos de despesa são mais representativos?
- Quais fornecedores mais receberam pagamentos?
- Como os gastos evoluíram ao longo dos meses?

## Tecnologias utilizadas

- Databricks Free Edition
- Apache Spark
- PySpark
- SQL
- Delta Lake
- Unity Catalog
- Volumes do Databricks
- Dados Abertos da Câmara dos Deputados
- Power BI, como etapa futura de visualização

## Fonte dos dados

Os dados utilizados foram obtidos no portal de Dados Abertos da Câmara dos Deputados.

Base utilizada:

```text
Cota para Exercício da Atividade Parlamentar, CEAP
Ano: 2024
Formato: CSV compactado em ZIP
