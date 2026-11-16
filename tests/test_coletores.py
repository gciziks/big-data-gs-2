import json
import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "dags"))
sys.path.insert(0, str(RAIZ / "tests"))
import coletores as col  # noqa: E402
from fakes import DIA_TESTE, falso_get_json  # noqa: E402


@pytest.fixture(autouse=True)
def sem_internet(monkeypatch):
    monkeypatch.setattr(col, "_get_json", falso_get_json)


def test_municipios_validos():
    m = col.ler_municipios()
    assert len(m) == 12
    assert all(len(x["codigo_ibge"]) == 7 for x in m)
    assert len({x["codigo_ibge"] for x in m}) == 12


def test_clima_uma_linha_por_municipio():
    m = col.ler_municipios()
    linhas = col.coletar_clima(DIA_TESTE, m, pausa=0)
    assert len(linhas) == 12
    assert linhas[0]["codigo_ibge"] == "3550308"
    assert linhas[0]["data_referencia"] == "2026-11-10"
    assert len(linhas[0]["hourly"]["time"]) == 24


def test_ibge_uma_linha_por_municipio():
    m = col.ler_municipios()
    linhas = col.coletar_ibge(m)
    assert [l["codigo_ibge"] for l in linhas] == [x["codigo_ibge"] for x in m]
    serie = linhas[0]["resposta"][0]["resultados"][0]["series"][0]
    assert serie["localidade"]["id"] == "3550308" and "2022" in serie["serie"]


def test_ibge_falha_se_faltar_municipio(monkeypatch):
    def incompleto(url, **kw):
        r = falso_get_json(url, **kw)
        r[0]["resultados"][0]["series"].pop()
        return r
    monkeypatch.setattr(col, "_get_json", incompleto)
    with pytest.raises(RuntimeError, match="não devolveu"):
        col.coletar_ibge(col.ler_municipios())


def test_inscricoes_mesma_data_gera_mesmos_ids_e_defeitos():
    m = col.ler_municipios()
    a, b = col.gerar_inscricoes(DIA_TESTE, m), col.gerar_inscricoes(DIA_TESTE, m)
    assert [x["id_inscricao"] for x in a] == [x["id_inscricao"] for x in b]
    assert [x["email"] for x in a] == [x["email"] for x in b]
    assert all(x["email"].endswith("example.com") or "@" not in x["email"] for x in a)


def test_gravar_e_conferir(tmp_path):
    destino = tmp_path / "clima" / "clima_2026-11-10.jsonl"
    n = col.gravar_jsonl(col.coletar_clima(DIA_TESTE, col.ler_municipios()[:2], pausa=0), destino)
    assert n == 2 and col.contar_linhas_validas(destino) == 2
    assert not list(destino.parent.glob("*.tmp"))


def test_conferir_detecta_problemas(tmp_path):
    with pytest.raises(FileNotFoundError):
        col.contar_linhas_validas(tmp_path / "nao_existe.jsonl")
    vazio = tmp_path / "vazio.jsonl"; vazio.write_text("")
    with pytest.raises(ValueError, match="vazio"):
        col.contar_linhas_validas(vazio)
    ruim = tmp_path / "ruim.jsonl"; ruim.write_text('{"a":1}\nisso nao e json\n')
    with pytest.raises(ValueError, match="não é JSON"):
        col.contar_linhas_validas(ruim)
