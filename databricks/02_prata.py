# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata: dado tratado (requisito 06)
# MAGIC - Converte para os **tipos corretos** (data, número).
# MAGIC - Trata **nulos e inválidos** com regras de qualidade e **conta quantos registros cada regra barrou**.
# MAGIC - Grava com **MERGE pela chave**: rodar de novo atualiza o que já existe e insere só o que é novo.
# MAGIC
# MAGIC Ordem: IBGE, inscrições, clima (as inscrições usam a lista de municípios do IBGE).

# COMMAND ----------
# [funcoes]
# Funções puras (só transformam tabelas). Ficam numa célula à parte para poderem ser testadas fora do Databricks.
from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def _json_como_struct(df, coluna, schema):
    """O Auto Loader pode guardar campos aninhados (listas e objetos) como TEXTO JSON na Bronze.
    Aqui o texto é aberto com um schema explícito. Se a coluna já for uma estrutura, funciona igual."""
    eh_texto = isinstance(df.schema[coluna].dataType, StringType)
    texto = F.col(coluna) if eh_texto else F.to_json(F.col(coluna))
    return F.from_json(texto, schema)


def parse_clima(bronze):
    """Uma linha por município e por hora, com tipos corretos.
    A API devolve listas paralelas (hora, temperatura, chuva): arrays_zip junta e explode abre."""
    h = _json_como_struct(
        bronze, "hourly",
        "struct<time:array<string>,temperature_2m:array<string>,precipitation:array<string>>")
    zip_ = F.arrays_zip(
        h["time"].alias("hora"),
        h["temperature_2m"].alias("temperatura_c"),
        h["precipitation"].alias("precipitacao_mm"),
    )
    return (
        bronze.select(
            F.col("codigo_ibge").cast("string").alias("codigo_ibge"),
            F.col("municipio"),
            F.col("carregado_em"),
            F.explode(zip_).alias("x"),
        )
        .select(
            "codigo_ibge",
            "municipio",
            F.to_timestamp(F.col("x.hora"), "yyyy-MM-dd'T'HH:mm").alias("data_hora"),
            F.col("x.temperatura_c").cast("double").alias("temperatura_c"),
            F.col("x.precipitacao_mm").cast("double").alias("precipitacao_mm"),
            "carregado_em",
        )
    )


def parse_ibge(bronze):
    """Uma linha por município com a população. O ano vem como nome de campo ({"2022": "..."}),
    então viramos o campo em mapa para ler ano e valor sem depender do ano."""
    resp = _json_como_struct(
        bronze, "resposta",
        "array<struct<resultados:array<struct<series:array<struct<"
        "localidade:struct<id:string,nome:string>,serie:map<string,string>>>>>>>")
    serie = resp[0]["resultados"][0]["series"][0]
    mapa = serie["serie"]
    nome_uf = serie["localidade"]["nome"]
    return bronze.select(
        F.col("codigo_ibge").cast("string").alias("codigo_ibge"),
        F.regexp_extract(nome_uf, r"^(.*) - ([A-Z]{2})$", 1).alias("nome"),
        F.regexp_extract(nome_uf, r"^(.*) - ([A-Z]{2})$", 2).alias("uf"),
        F.map_keys(mapa)[0].cast("int").alias("ano_referencia"),
        F.map_values(mapa)[0].cast("long").alias("populacao"),
        "carregado_em",
    )


def parse_inscricoes(bronze):
    return bronze.select(
        "id_inscricao", "nome", "email", "telefone",
        F.col("codigo_ibge").cast("string").alias("codigo_ibge"),
        "canal",
        F.to_date("data_inscricao").alias("data_inscricao"),
        "carregado_em",
    )


def dedup(df, chaves):
    """Se a mesma chave aparece em mais de um arquivo, fica a linha mais recente (o MERGE exige 1 por chave)."""
    w = Window.partitionBy(*chaves).orderBy(F.col("carregado_em").desc())
    return df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n")


def aplicar_regras(df, regras):
    """regras = {nome_da_regra: condição que é VERDADEIRA quando o registro é ruim}.
    Devolve (linhas_boas, contagens), onde contagens traz o total lido e o total barrado por regra.
    Um registro pode quebrar mais de uma regra; ele é barrado se quebrar qualquer uma."""
    marcado = df
    for nome, ruim in regras.items():
        marcado = marcado.withColumn(nome, F.coalesce(ruim, F.lit(True)))
    soma = [F.sum(F.col(n).cast("int")).alias(n) for n in regras]
    contagens = marcado.agg(F.count(F.lit(1)).alias("total_lido"), *soma).first().asDict()
    contagens = {k: int(v or 0) for k, v in contagens.items()}
    alguma_ruim = reduce(lambda a, b: a | b, [F.col(n) for n in regras])
    boas = marcado.filter(~alguma_ruim).drop(*regras.keys())
    return boas, contagens


# COMMAND ----------
# MAGIC %md
# MAGIC ## Tabelas de destino e registro de qualidade

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS workspace.gs.prata_ibge (
  codigo_ibge STRING, nome STRING, uf STRING, ano_referencia INT, populacao BIGINT, atualizado_em TIMESTAMP)""")
spark.sql("""CREATE TABLE IF NOT EXISTS workspace.gs.prata_inscricoes (
  id_inscricao STRING, nome STRING, email STRING, telefone STRING, codigo_ibge STRING,
  canal STRING, data_inscricao DATE, atualizado_em TIMESTAMP)""")
spark.sql("""CREATE TABLE IF NOT EXISTS workspace.gs.prata_clima (
  codigo_ibge STRING, municipio STRING, data_hora TIMESTAMP, temperatura_c DOUBLE,
  precipitacao_mm DOUBLE, atualizado_em TIMESTAMP)""")
spark.sql("""CREATE TABLE IF NOT EXISTS workspace.gs.prata_qualidade (
  executado_em TIMESTAMP, tabela STRING, regra STRING, total_lido BIGINT, total_barrado BIGINT)""")


def merge(lote, tabela, chaves):
    """MERGE pela chave: atualiza o que existe, insere o que é novo."""
    view = "lote_" + tabela.split(".")[-1]
    lote.withColumn("atualizado_em", F.current_timestamp()).createOrReplaceTempView(view)
    condicao = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    spark.sql(f"""
      MERGE INTO {tabela} AS alvo USING {view} AS novo ON {condicao}
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")


def registrar_qualidade(tabela, contagens):
    total = contagens["total_lido"]
    linhas = [(tabela, regra, total, barrados) for regra, barrados in contagens.items() if regra != "total_lido"]
    (spark.createDataFrame(linhas, "tabela STRING, regra STRING, total_lido BIGINT, total_barrado BIGINT")
          .withColumn("executado_em", F.current_timestamp())
          .select("executado_em", "tabela", "regra", "total_lido", "total_barrado")
          .write.mode("append").saveAsTable("workspace.gs.prata_qualidade"))
    for regra, barrados in contagens.items():
        if regra != "total_lido":
            print(f"{tabela} | {regra}: {barrados} de {total} barrados")


# COMMAND ----------
# MAGIC %md
# MAGIC ## 1) IBGE: população (3 regras de qualidade)

# COMMAND ----------

ibge = parse_ibge(spark.table("workspace.gs.bronze_ibge"))
regras_ibge = {
    "R1_populacao_nula_ou_zero": F.col("populacao").isNull() | (F.col("populacao") <= 0),
    "R2_codigo_ibge_fora_do_padrao": ~F.col("codigo_ibge").rlike(r"^[0-9]{7}$"),
    "R3_ano_de_referencia_nulo": F.col("ano_referencia").isNull(),
}
ibge_ok, cont = aplicar_regras(ibge, regras_ibge)
registrar_qualidade("prata_ibge", cont)
merge(dedup(ibge_ok, ["codigo_ibge"]).drop("carregado_em"), "workspace.gs.prata_ibge", ["codigo_ibge"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2) Inscrições simuladas (3 regras de qualidade)

# COMMAND ----------

codigos_validos = [r.codigo_ibge for r in spark.table("workspace.gs.prata_ibge").select("codigo_ibge").collect()]
assert codigos_validos, "Rode a parte do IBGE antes: a Prata do IBGE está vazia."

insc = parse_inscricoes(spark.table("workspace.gs.bronze_inscricoes"))
regras_insc = {
    "R1_email_invalido": ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    "R2_telefone_invalido": ~F.col("telefone").rlike(r"^[0-9]{11}$"),
    "R3_municipio_desconhecido": ~F.col("codigo_ibge").isin(codigos_validos),
}
insc_ok, cont = aplicar_regras(insc, regras_insc)
registrar_qualidade("prata_inscricoes", cont)
merge(dedup(insc_ok, ["id_inscricao"]).drop("carregado_em"), "workspace.gs.prata_inscricoes", ["id_inscricao"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3) Clima por hora (3 regras de qualidade)

# COMMAND ----------

clima = parse_clima(spark.table("workspace.gs.bronze_clima"))
regras_clima = {
    "R1_chuva_nula_ou_negativa": F.col("precipitacao_mm").isNull() | (F.col("precipitacao_mm") < 0),
    "R2_temperatura_fora_da_faixa": F.col("temperatura_c").isNull() | ~F.col("temperatura_c").between(-10, 50),
    "R3_hora_invalida": F.col("data_hora").isNull(),
}
clima_ok, cont = aplicar_regras(clima, regras_clima)
registrar_qualidade("prata_clima", cont)
merge(dedup(clima_ok, ["codigo_ibge", "data_hora"]).drop("carregado_em"),
      "workspace.gs.prata_clima", ["codigo_ibge", "data_hora"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resultado (tire print destas tabelas)

# COMMAND ----------

display(spark.sql("""
  SELECT executado_em, tabela, regra, total_lido, total_barrado
  FROM workspace.gs.prata_qualidade ORDER BY executado_em DESC, tabela, regra"""))

# COMMAND ----------

display(spark.sql("""
  SELECT 'prata_clima' AS tabela, COUNT(*) AS linhas FROM workspace.gs.prata_clima
  UNION ALL SELECT 'prata_ibge', COUNT(*) FROM workspace.gs.prata_ibge
  UNION ALL SELECT 'prata_inscricoes', COUNT(*) FROM workspace.gs.prata_inscricoes"""))
