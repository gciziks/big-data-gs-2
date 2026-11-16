# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: dado bruto, só o que é novo (requisitos 04 e 05)
# MAGIC O **Auto Loader** lê os arquivos do Volume e lembra quais já leu (no *checkpoint*).
# MAGIC Por isso o 2º lote traz só os arquivos novos e rodar de novo não duplica nada.
# MAGIC
# MAGIC A Bronze guarda tudo como veio, **em texto** (os tipos corretos aparecem na Prata).
# MAGIC
# MAGIC **Como usar para o relatório:** rode depois do lote 1 (`rotulo = apos_lote_1`), suba o lote 2 e rode de novo
# MAGIC (`apos_lote_2`), e rode uma terceira vez sem subir nada (`reexecucao`). Tire print da tabela de contagens no final.

# COMMAND ----------

dbutils.widgets.text("rotulo", "apos_lote_1", "Rótulo desta execução")
ROTULO = dbutils.widgets.get("rotulo")

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/gs/chegada"
FONTES = ["clima", "ibge", "inscricoes"]

# COMMAND ----------

def carregar_bronze(fonte: str):
    """Lê só os arquivos novos de /chegada/<fonte> e acrescenta na tabela bronze_<fonte>."""
    consulta = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .trigger(availableNow=True)  # processa o que há de novo e termina
        .toTable(f"workspace.gs.bronze_{fonte}")
    )
    consulta.awaitTermination()


for f in FONTES:
    carregar_bronze(f)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Contagens (antes e depois)
# MAGIC Guarda quantas linhas e quantos arquivos cada tabela tem, a cada execução. É a prova de "só o novo entra, sem duplicar".

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.gs.bronze_contagens (
  rotulo STRING, tabela STRING, linhas BIGINT, arquivos BIGINT, registrado_em TIMESTAMP)
""")

for f in FONTES:
    tabela = f"workspace.gs.bronze_{f}"
    spark.sql(f"""
      INSERT INTO workspace.gs.bronze_contagens
      SELECT '{ROTULO}', '{tabela}', COUNT(*), COUNT(DISTINCT arquivo_origem), current_timestamp()
      FROM {tabela}
    """)

display(spark.sql("SELECT * FROM workspace.gs.bronze_contagens ORDER BY registrado_em, tabela"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Conferir de onde veio cada linha

# COMMAND ----------

display(spark.sql("""
  SELECT 'clima' AS fonte, arquivo_origem, COUNT(*) AS linhas FROM workspace.gs.bronze_clima GROUP BY arquivo_origem
  UNION ALL SELECT 'ibge', arquivo_origem, COUNT(*) FROM workspace.gs.bronze_ibge GROUP BY arquivo_origem
  UNION ALL SELECT 'inscricoes', arquivo_origem, COUNT(*) FROM workspace.gs.bronze_inscricoes GROUP BY arquivo_origem
  ORDER BY fonte, arquivo_origem
"""))
