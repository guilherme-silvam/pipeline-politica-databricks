# Databricks notebook source
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    to_date,
    current_timestamp,
    when
)

df_bronze = spark.table("workspace.politica_bronze.despesas_ceap")

df_silver = (
    df_bronze
    .filter(col("numAno").isNotNull())
    .select(
        initcap(trim(col("txNomeParlamentar"))).alias("nome_parlamentar"),
        col("cpf").cast("string").alias("cpf"),
        col("ideCadastro").cast("string").alias("id_cadastro"),
        col("nuCarteiraParlamentar").cast("int").alias("numero_carteira_parlamentar"),
        col("nuLegislatura").cast("int").alias("legislatura"),
        upper(trim(col("sgUF"))).alias("uf"),
        upper(trim(col("sgPartido"))).alias("partido"),
        col("codLegislatura").cast("int").alias("codigo_legislatura"),
        col("numSubCota").cast("int").alias("codigo_tipo_despesa"),
        upper(trim(col("txtDescricao"))).alias("tipo_despesa"),
        col("numEspecificacaoSubCota").cast("int").alias("codigo_especificacao_despesa"),
        upper(trim(col("txtDescricaoEspecificacao"))).alias("especificacao_despesa"),
        upper(trim(col("txtFornecedor"))).alias("fornecedor"),
        col("txtCNPJCPF").cast("string").alias("cnpj_cpf_fornecedor"),
        col("txtNumero").cast("string").alias("numero_documento"),
        col("indTipoDocumento").cast("int").alias("tipo_documento"),
        to_date(col("datEmissao")).alias("data_emissao"),
        col("vlrDocumento").cast("double").alias("valor_documento"),
        col("vlrGlosa").cast("double").alias("valor_glosa"),
        col("vlrLiquido").cast("double").alias("valor_liquido"),
        col("numMes").cast("int").alias("mes"),
        col("numAno").cast("int").alias("ano"),
        col("numParcela").cast("int").alias("numero_parcela"),
        upper(trim(col("txtPassageiro"))).alias("passageiro"),
        upper(trim(col("txtTrecho"))).alias("trecho"),
        col("numLote").cast("string").alias("numero_lote"),
        col("numRessarcimento").cast("string").alias("numero_ressarcimento"),
        col("vlrRestituicao").cast("double").alias("valor_restituicao"),
        col("nuDeputadoId").cast("string").alias("id_deputado"),
        col("ideDocumento").cast("string").alias("id_documento"),
        col("urlDocumento").alias("url_documento")
    )
    .withColumn(
        "fornecedor",
        when(col("fornecedor").isNull(), "NÃO INFORMADO").otherwise(col("fornecedor"))
    )
    .withColumn(
        "tipo_despesa",
        when(col("tipo_despesa").isNull(), "NÃO INFORMADO").otherwise(col("tipo_despesa"))
    )
    .withColumn(
        "valor_liquido",
        when(col("valor_liquido").isNull(), 0).otherwise(col("valor_liquido"))
    )
    .withColumn("_data_processamento", current_timestamp())
)

display(df_silver.limit(20))

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.politica_silver.despesas_ceap")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.politica_silver.despesas_ceap
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ano,
# MAGIC   partido,
# MAGIC   uf,
# MAGIC   COUNT(*) AS quantidade_despesas,
# MAGIC   ROUND(SUM(valor_liquido), 2) AS total_gasto
# MAGIC FROM workspace.politica_silver.despesas_ceap
# MAGIC GROUP BY ano, partido, uf
# MAGIC ORDER BY total_gasto DESC
# MAGIC LIMIT 20;