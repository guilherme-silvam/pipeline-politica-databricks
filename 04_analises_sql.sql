-- Databricks notebook source
SHOW TABLES IN workspace.politica_gold;

-- COMMAND ----------

SELECT
  ROUND(SUM(total_gasto), 2) AS total_gasto,
  SUM(quantidade_despesas) AS total_despesas,
  ROUND(AVG(ticket_medio), 2) AS ticket_medio_geral
FROM workspace.politica_gold.gastos_por_parlamentar;

-- COMMAND ----------

SELECT
  nome_parlamentar,
  partido,
  uf,
  total_gasto,
  quantidade_despesas,
  ticket_medio
FROM workspace.politica_gold.gastos_por_parlamentar
ORDER BY total_gasto DESC
LIMIT 10;

-- COMMAND ----------

SELECT
  partido,
  total_gasto,
  quantidade_despesas,
  ticket_medio
FROM workspace.politica_gold.gastos_por_partido
ORDER BY total_gasto DESC;

-- COMMAND ----------

SELECT
  uf,
  total_gasto,
  quantidade_despesas,
  ticket_medio
FROM workspace.politica_gold.gastos_por_uf
ORDER BY total_gasto DESC;

-- COMMAND ----------

SELECT
  tipo_despesa,
  total_gasto,
  quantidade_despesas,
  ticket_medio
FROM workspace.politica_gold.gastos_por_tipo_despesa
ORDER BY total_gasto DESC
LIMIT 15;

-- COMMAND ----------

SELECT
  ano,
  mes,
  total_gasto,
  quantidade_despesas
FROM workspace.politica_gold.evolucao_mensal_gastos
ORDER BY ano, mes;

-- COMMAND ----------

SELECT
  fornecedor,
  cnpj_cpf_fornecedor,
  total_recebido,
  quantidade_despesas,
  ticket_medio
FROM workspace.politica_gold.top_fornecedores
ORDER BY total_recebido DESC
LIMIT 20;

-- COMMAND ----------

SELECT
  partido,
  COUNT(nome_parlamentar) AS quantidade_parlamentares,
  ROUND(SUM(total_gasto), 2) AS total_gasto_partido,
  ROUND(SUM(total_gasto) / COUNT(nome_parlamentar), 2) AS gasto_medio_por_parlamentar
FROM workspace.politica_gold.gastos_por_parlamentar
GROUP BY partido
ORDER BY gasto_medio_por_parlamentar DESC;