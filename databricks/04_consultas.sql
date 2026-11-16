-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · As 5 consultas SQL (requisito 07)
-- MAGIC Cada consulta responde uma pergunta do MVP. Depois de rodar, **escreva a interpretação do resultado no relatório**
-- MAGIC (o que o número quer dizer para o cidadão). A consulta 5 é a de **impacto**.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Consulta 1 · Quais municípios mais choveram no período?
-- MAGIC *Pergunta do MVP: onde choveu mais, para o cidadão saber onde o risco foi maior?*

-- COMMAND ----------

SELECT
  m.nome_municipio,
  ROUND(SUM(f.chuva_total_mm), 1) AS chuva_acumulada_mm,
  ROUND(MAX(f.chuva_max_hora_mm), 1) AS maior_chuva_em_1_hora_mm,
  COUNT(*) AS dias_observados
FROM workspace.gs.fato_chuva_dia f
JOIN workspace.gs.dim_municipio m ON m.codigo_ibge = f.codigo_ibge
GROUP BY m.nome_municipio
ORDER BY chuva_acumulada_mm DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Consulta 2 · Quantos dias cada município ficou em cada nível de alerta?
-- MAGIC *Pergunta do MVP: com que frequência o alerta é forte em cada cidade?*

-- COMMAND ----------

SELECT
  m.nome_municipio,
  SUM(CASE WHEN f.id_nivel_alerta = 1 THEN 1 ELSE 0 END) AS dias_baixo,
  SUM(CASE WHEN f.id_nivel_alerta = 2 THEN 1 ELSE 0 END) AS dias_moderado,
  SUM(CASE WHEN f.id_nivel_alerta = 3 THEN 1 ELSE 0 END) AS dias_forte,
  SUM(CASE WHEN f.id_nivel_alerta = 4 THEN 1 ELSE 0 END) AS dias_muito_forte
FROM workspace.gs.fato_chuva_dia f
JOIN workspace.gs.dim_municipio m ON m.codigo_ibge = f.codigo_ibge
GROUP BY m.nome_municipio
ORDER BY dias_muito_forte DESC, dias_forte DESC, m.nome_municipio;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Consulta 3 · Chove mais em dia útil ou no fim de semana?
-- MAGIC *Pergunta do MVP: em que dias da semana o cidadão deve ficar mais atento?*

-- COMMAND ----------

SELECT
  d.dia_semana,
  ROUND(AVG(f.chuva_total_mm), 2) AS chuva_media_mm,
  ROUND(MAX(f.chuva_total_mm), 1) AS maior_chuva_do_dia_mm,
  COUNT(*) AS registros
FROM workspace.gs.fato_chuva_dia f
JOIN workspace.gs.dim_data d ON d.data = f.data
GROUP BY d.dia_semana, d.dia_semana_num
ORDER BY d.dia_semana_num;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Consulta 4 · A chuva muda conforme o tamanho da cidade?
-- MAGIC *Pergunta do MVP: cidades grandes e pequenas da região recebem chuva parecida?*

-- COMMAND ----------

SELECT
  m.faixa_populacao,
  COUNT(DISTINCT m.codigo_ibge) AS municipios,
  ROUND(AVG(f.chuva_total_mm), 2) AS chuva_media_dia_mm,
  ROUND(AVG(f.temp_max_c), 1) AS temp_max_media_c
FROM workspace.gs.fato_chuva_dia f
JOIN workspace.gs.dim_municipio m ON m.codigo_ibge = f.codigo_ibge
GROUP BY m.faixa_populacao
ORDER BY chuva_media_dia_mm DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Consulta 5 (IMPACTO) · Quantas pessoas estavam sob chuva e quantas inscritas seriam avisadas?
-- MAGIC *Pergunta do MVP: qual o alcance do alerta? Quantos moradores vivem em municípios com chuva moderada ou pior,
-- MAGIC e quantos cidadãos inscritos receberiam o aviso?*
-- MAGIC
-- MAGIC Está em **Moderado ou pior** (`id_nivel_alerta >= 2`) para sempre haver resultado. Troque para `>= 3` para ver só
-- MAGIC chuva forte. Usa a tabela `ouro_painel_alerta`, a mesma que o MVP lê.

-- COMMAND ----------

SELECT
  data,
  COUNT(*) AS municipios_com_chuva,
  SUM(populacao) AS populacao_sob_chuva,
  SUM(qtd_inscritos) AS inscritos_na_regiao,
  ROUND(100.0 * SUM(populacao) / (SELECT SUM(populacao) FROM workspace.gs.dim_municipio), 1) AS pct_da_populacao_da_regiao
FROM workspace.gs.ouro_painel_alerta
WHERE id_nivel_alerta >= 2
GROUP BY data
ORDER BY data;
