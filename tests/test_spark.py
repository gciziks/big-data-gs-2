"""Testa, com Spark local e dados simulados, as regras da Prata e as tabelas/consultas da Ouro.

NÃO testa o que só existe no Databricks: Auto Loader (readStream cloudFiles), Delta e o comando MERGE.
Para os testes, os arquivos .jsonl são lidos como texto ("primitivesAsString"), igual ao Auto Loader faz
por padrão, e as tabelas da Ouro viram views temporárias.
"""
import json
import re
import sys
from datetime import date
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "dags"))
sys.path.insert(0, str(RAIZ / "tests"))
import coletores as col  # noqa: E402
from fakes import falso_get_json  # noqa: E402

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

DIAS = [date(2026, 11, 9), date(2026, 11, 10)]


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("teste_gs")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "America/Sao_Paulo").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def notebook():
    """Executa só a célula '# [funcoes]' do notebook da Prata e devolve o namespace."""
    texto = (RAIZ / "databricks" / "02_prata.py").read_text(encoding="utf-8")
    celula = next(c for c in texto.split("# COMMAND ----------") if "# [funcoes]" in c)
    ns = {}
    exec(compile(celula, "02_prata.py[funcoes]", "exec"), ns)
    return ns


@pytest.fixture(scope="module")
def chegada(tmp_path_factory):
    """Cria 2 dias de arquivos simulados e injeta defeitos conhecidos no clima."""
    mp = pytest.MonkeyPatch()
    mp.setattr(col, "_get_json", falso_get_json)
    pasta = tmp_path_factory.mktemp("chegada")
    municipios = col.ler_municipios()
    for dia in DIAS:
        col.gravar_jsonl(col.coletar_clima(dia, municipios, pausa=0), pasta / "clima" / f"clima_{dia}.jsonl")
        col.gravar_jsonl(col.gerar_inscricoes(dia, municipios), pasta / "inscricoes" / f"inscricoes_{dia}.jsonl")
    col.gravar_jsonl(col.coletar_ibge(municipios), pasta / "ibge" / f"ibge_{DIAS[-1]}.jsonl")

    # defeitos conhecidos no clima de 10/11: Guarulhos (chuva nula e negativa, temperatura absurda),
    # Osasco com chuva forte (6 mm/h x 4h = 24 mm) e Barueri com chuva muito forte (15 mm/h x 4h = 60 mm)
    arq = pasta / "clima" / f"clima_{DIAS[-1]}.jsonl"
    linhas = [json.loads(l) for l in arq.read_text(encoding="utf-8").splitlines()]
    for l in linhas:
        if l["codigo_ibge"] == "3518800":
            l["hourly"]["precipitation"][0] = None
            l["hourly"]["precipitation"][1] = -2.0
            l["hourly"]["temperature_2m"][2] = 99.0
        if l["codigo_ibge"] == "3534401":
            l["hourly"]["precipitation"] = [6.0 if 14 <= h <= 17 else 0.0 for h in range(24)]
        if l["codigo_ibge"] == "3505708":
            l["hourly"]["precipitation"] = [15.0 if 14 <= h <= 17 else 0.0 for h in range(24)]
    col.gravar_jsonl(linhas, arq)
    mp.undo()
    return pasta


def ler_bronze(spark, pasta, fonte):
    """Imita a Bronze: tudo como texto + arquivo de origem + carregado_em."""
    df = spark.read.option("primitivesAsString", "true").json(str(pasta / fonte))
    return (df.withColumn("arquivo_origem", F.input_file_name())
              .withColumn("carregado_em", F.current_timestamp()))


# ---------------------------------------------------------------- Prata
def test_prata_clima(spark, notebook, chegada):
    bronze = ler_bronze(spark, chegada, "clima")
    assert bronze.count() == 24  # 12 municípios x 2 dias, 1 linha por município/dia
    clima = notebook["parse_clima"](bronze)
    assert clima.count() == 24 * 24
    tipos = dict(clima.dtypes)
    assert tipos["data_hora"] == "timestamp" and tipos["precipitacao_mm"] == "double" and tipos["temperatura_c"] == "double"

    regras = {
        "R1_chuva_nula_ou_negativa": F.col("precipitacao_mm").isNull() | (F.col("precipitacao_mm") < 0),
        "R2_temperatura_fora_da_faixa": F.col("temperatura_c").isNull() | ~F.col("temperatura_c").between(-10, 50),
        "R3_hora_invalida": F.col("data_hora").isNull(),
    }
    boas, cont = notebook["aplicar_regras"](clima, regras)
    assert cont == {"total_lido": 576, "R1_chuva_nula_ou_negativa": 2, "R2_temperatura_fora_da_faixa": 1, "R3_hora_invalida": 0}
    assert boas.count() == 576 - 3  # 3 linhas diferentes foram barradas


def test_dedup_mantem_a_linha_mais_recente(spark, notebook, chegada):
    clima = notebook["parse_clima"](ler_bronze(spark, chegada, "clima"))
    dobrado = clima.unionByName(clima.withColumn("carregado_em", F.col("carregado_em") + F.expr("INTERVAL 1 HOUR")))
    assert dobrado.count() == 2 * clima.count()
    unico = notebook["dedup"](dobrado, ["codigo_ibge", "data_hora"])
    assert unico.count() == clima.count()


def test_prata_ibge(spark, notebook, chegada):
    ibge = notebook["parse_ibge"](ler_bronze(spark, chegada, "ibge")).collect()
    assert len(ibge) == 12
    sp = next(r for r in ibge if r.codigo_ibge == "3550308")
    assert (sp.nome, sp.uf, sp.ano_referencia) == ("Cidade 308", "SP", 2022)
    assert sp.populacao == 100000 + 308 * 1000


def test_prata_inscricoes_regras_batem_com_python(spark, notebook, chegada):
    brutos = []
    for dia in DIAS:
        brutos += col.gerar_inscricoes(dia, col.ler_municipios())
    esperado_email = sum(1 for r in brutos if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", r["email"]))
    esperado_tel = sum(1 for r in brutos if not re.match(r"^[0-9]{11}$", r["telefone"]))
    assert esperado_email + esperado_tel > 0  # os defeitos propositais existem

    insc = notebook["parse_inscricoes"](ler_bronze(spark, chegada, "inscricoes"))
    codigos = [m["codigo_ibge"] for m in col.ler_municipios()]
    regras = {
        "R1_email_invalido": ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
        "R2_telefone_invalido": ~F.col("telefone").rlike(r"^[0-9]{11}$"),
        "R3_municipio_desconhecido": ~F.col("codigo_ibge").isin(codigos),
    }
    boas, cont = notebook["aplicar_regras"](insc, regras)
    assert cont["total_lido"] == len(brutos)
    assert cont["R1_email_invalido"] == esperado_email and cont["R2_telefone_invalido"] == esperado_tel
    assert cont["R3_municipio_desconhecido"] == 0
    assert boas.count() < len(brutos)


# ---------------------------------------------------------------- Ouro
def _celulas_sql(arquivo):
    texto = (RAIZ / "databricks" / arquivo).read_text(encoding="utf-8")
    saida = []
    for celula in texto.split("-- COMMAND ----------"):
        linhas = [l for l in celula.splitlines() if not l.strip().startswith("-- MAGIC") and l.strip() != "-- Databricks notebook source"]
        sql = "\n".join(linhas).strip()
        if re.search(r"\b(CREATE|SELECT|WITH)\b", re.sub(r"--[^\n]*", "", sql)):
            sql = sql.rstrip().rstrip(";")
            sql = sql.replace("workspace.gs.", "gs_").replace("CREATE OR REPLACE TABLE", "CREATE OR REPLACE TEMP VIEW")
            saida.append(sql)
    return saida


@pytest.fixture(scope="module")
def ouro(spark, notebook, chegada):
    clima = notebook["parse_clima"](ler_bronze(spark, chegada, "clima"))
    clima_ok, _ = notebook["aplicar_regras"](clima, {
        "R1": F.col("precipitacao_mm").isNull() | (F.col("precipitacao_mm") < 0),
        "R2": F.col("temperatura_c").isNull() | ~F.col("temperatura_c").between(-10, 50)})
    notebook["dedup"](clima_ok, ["codigo_ibge", "data_hora"]).createOrReplaceTempView("gs_prata_clima")
    notebook["dedup"](notebook["parse_ibge"](ler_bronze(spark, chegada, "ibge")), ["codigo_ibge"]).createOrReplaceTempView("gs_prata_ibge")
    insc = notebook["parse_inscricoes"](ler_bronze(spark, chegada, "inscricoes"))
    insc_ok, _ = notebook["aplicar_regras"](insc, {
        "R1": ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), "R2": ~F.col("telefone").rlike(r"^[0-9]{11}$")})
    notebook["dedup"](insc_ok, ["id_inscricao"]).createOrReplaceTempView("gs_prata_inscricoes")
    for sql in _celulas_sql("03_ouro.sql"):
        spark.sql(sql)
    return spark


def test_ouro_fato_e_dimensoes(ouro):
    q = lambda s: ouro.sql(s).collect()
    assert q("SELECT COUNT(*) c FROM gs_fato_chuva_dia")[0].c == 24  # 12 municípios x 2 dias
    assert q("SELECT COUNT(*) c FROM gs_dim_municipio")[0].c == 12
    assert q("SELECT COUNT(*) c FROM gs_dim_data")[0].c == 2
    assert q("SELECT COUNT(*) c FROM gs_dim_nivel_alerta")[0].c == 4
    # nenhuma chave órfã e nenhum nível nulo
    assert q("SELECT COUNT(*) c FROM gs_fato_chuva_dia f LEFT JOIN gs_dim_municipio m USING (codigo_ibge) WHERE m.nome_municipio IS NULL")[0].c == 0
    assert q("SELECT COUNT(*) c FROM gs_fato_chuva_dia WHERE id_nivel_alerta IS NULL")[0].c == 0
    # níveis esperados no dia 10/11: Osasco 24 mm = Forte (3), Barueri 60 mm = Muito forte (4)
    niveis = {r.codigo_ibge: r.id_nivel_alerta for r in q("SELECT codigo_ibge, id_nivel_alerta FROM gs_fato_chuva_dia WHERE data = DATE'2026-11-10'")}
    assert niveis["3534401"] == 3 and niveis["3505708"] == 4
    assert q("SELECT dia_semana FROM gs_dim_data WHERE data = DATE'2026-11-10'")[0].dia_semana == "Terça-feira"


def test_ouro_lgpd_nao_vaza_dado_pessoal(ouro):
    linhas = ouro.sql("SELECT * FROM gs_dim_inscrito").collect()
    assert linhas
    for r in linhas:
        assert re.fullmatch(r"[0-9a-f]{64}", r.email_hash)
        assert re.fullmatch(r"\(11\) \*{5}-\d{4}", r.telefone_mascarado)
        assert re.fullmatch(r"[A-Za-zÀ-ú]+ [A-Z]\.", r.nome_mascarado)
        assert "@" not in "".join(str(v) for v in r)
    assert set(ouro.table("gs_dim_inscrito").columns).isdisjoint({"nome", "email", "telefone"})


def test_painel_do_mvp(ouro):
    painel = ouro.table("gs_ouro_painel_alerta")
    assert painel.count() == 24
    assert not {"nome", "email", "telefone", "email_hash"} & set(painel.columns)
    ruim = painel.filter("id_nivel_alerta < 3 AND inscritos_a_avisar > 0").count()
    assert ruim == 0
    forte = painel.filter("id_nivel_alerta >= 3").select("inscritos_a_avisar", "qtd_inscritos").collect()
    assert forte and all(r.inscritos_a_avisar == r.qtd_inscritos for r in forte)


def test_cinco_consultas_rodam_e_retornam_dados(ouro):
    consultas = [s for s in _celulas_sql("04_consultas.sql") if s.lstrip().upper().startswith(("SELECT", "WITH"))]
    assert len(consultas) == 5
    for i, sql in enumerate(consultas, start=1):
        linhas = ouro.sql(sql).collect()
        assert linhas, f"consulta {i} não retornou linhas"
    impacto = ouro.sql(consultas[4]).collect()
    assert {r.data.isoformat() for r in impacto} == {"2026-11-09", "2026-11-10"}
