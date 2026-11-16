"""Funções de coleta do projeto Alerta Chuva SP (sem dependência do Airflow).

Ficam separadas da DAG para poderem ser testadas sozinhas (veja scripts/testar_fontes.py).

Três fontes:
  1. Open-Meteo (API pública, sem chave): chuva e temperatura por hora, por município.
  2. IBGE, API de dados agregados (API pública, sem chave): população de cada município (Censo 2022).
  3. Inscrições de cidadãos (SIMULADAS): quem pediu para receber alerta de chuva. Os dados são
     fictícios e gerados por código com uma semente fixa por dia, para o pipeline poder
     demonstrar a proteção de dados pessoais (LGPD) sem usar pessoas reais.

Cada coleta vira UM arquivo .jsonl (uma linha JSON por registro) com a data no nome.
"""
from __future__ import annotations

import csv
import json
import os
import random
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

OPEN_METEO_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
IBGE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/4709/periodos/2022/variaveis/93"
FONTE_IBGE = "IBGE agregados tabela 4709, variavel 93 (populacao residente, Censo 2022)"
# Plano B: a mesma tabela do Censo 2022 pela API SIDRA do IBGE (outro servidor do mesmo instituto).
IBGE_SIDRA_URL = "https://apisidra.ibge.gov.br/values/t/4709/n6/{ids}/v/93/p/2022"
FONTE_IBGE_SIDRA = "IBGE SIDRA tabela 4709, variavel 93 (populacao residente, Censo 2022)"
TIMEZONE = "America/Sao_Paulo"
HEADERS = {"User-Agent": "gs-citizentech-fiap/1.0 (projeto academico)"}
# Alguns servidores derrubam conexões de programas desconhecidos. Da 2ª tentativa em diante,
# o pedido se identifica como um navegador comum.
HEADERS_NAVEGADOR = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json,text/plain,*/*",
}

MUNICIPIOS_CSV = Path(__file__).with_name("municipios.csv")


# ----------------------------------------------------------------------------------------------
# Utilitários
# ----------------------------------------------------------------------------------------------
def ler_municipios(caminho: Path | str | None = None) -> list[dict]:
    """Lê a lista de municípios (código IBGE, nome e coordenadas) do CSV."""
    with open(caminho or MUNICIPIOS_CSV, encoding="utf-8", newline="") as f:
        linhas = list(csv.DictReader(f))
    for m in linhas:
        m["latitude"] = float(m["latitude"])
        m["longitude"] = float(m["longitude"])
    return linhas


def agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_json(url: str, params: dict | None = None, tentativas: int = 3, sessao=None) -> dict | list:
    """GET com algumas tentativas. Falha com erro claro se a API não responder."""
    http = sessao or requests
    ultimo_erro: Exception | None = None
    for n in range(1, tentativas + 1):
        try:
            cabecalho = HEADERS if n == 1 else HEADERS_NAVEGADOR
            r = http.get(url, params=params, headers=cabecalho, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as erro:
            ultimo_erro = erro
            if n < tentativas:
                time.sleep(3 * n)
    raise RuntimeError(f"Falha ao consultar {url} depois de {tentativas} tentativas: {ultimo_erro}")


def gravar_jsonl(registros: list[dict], destino: Path) -> int:
    """Grava uma linha JSON por registro. Escreve num arquivo temporário e renomeia,
    para o Auto Loader nunca ler um arquivo pela metade."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_name(destino.name + ".tmp")
    with open(temporario, "w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
    os.replace(temporario, destino)
    return len(registros)


def contar_linhas_validas(caminho: Path | str) -> int:
    """Conta linhas JSON válidas de um arquivo. Levanta erro se o arquivo não existe, está
    vazio ou tem uma linha que não é JSON."""
    caminho = Path(caminho)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não chegou: {caminho}")
    if caminho.stat().st_size == 0:
        raise ValueError(f"Arquivo vazio: {caminho}")
    total = 0
    with open(caminho, encoding="utf-8") as f:
        for numero, linha in enumerate(f, start=1):
            if not linha.strip():
                continue
            try:
                json.loads(linha)
            except json.JSONDecodeError as erro:
                raise ValueError(f"Linha {numero} de {caminho.name} não é JSON: {erro}") from erro
            total += 1
    if total == 0:
        raise ValueError(f"Nenhuma linha válida em {caminho}")
    return total


# ----------------------------------------------------------------------------------------------
# Fonte 1: Open-Meteo (clima por hora)
# ----------------------------------------------------------------------------------------------
def coletar_clima(data_ref: date, municipios: list[dict], url: str = OPEN_METEO_URL,
                  pausa: float = 0.3, sessao=None) -> list[dict]:
    """Uma linha por município, com a resposta original da API + identificação do município."""
    saida = []
    for m in municipios:
        resposta = _get_json(url, sessao=sessao, params={
            "latitude": m["latitude"], "longitude": m["longitude"],
            "start_date": data_ref.isoformat(), "end_date": data_ref.isoformat(),
            "hourly": "temperature_2m,precipitation", "timezone": TIMEZONE,
        })
        if not isinstance(resposta, dict) or "hourly" not in resposta:
            raise RuntimeError(f"Resposta inesperada do Open-Meteo para {m['nome']}: {str(resposta)[:200]}")
        registro = dict(resposta)  # mantém tudo que a API devolveu
        registro.update({
            "codigo_ibge": m["codigo_ibge"], "municipio": m["nome"],
            "data_referencia": data_ref.isoformat(), "coletado_em": agora_iso(),
            "fonte": "Open-Meteo historical-forecast",
        })
        saida.append(registro)
        time.sleep(pausa)
    return saida


# ----------------------------------------------------------------------------------------------
# Fonte 2: IBGE (população)
# ----------------------------------------------------------------------------------------------
def _ibge_agregados(ids: list[str], url: str, sessao=None, lote: int = 4) -> dict[str, dict]:
    """API de agregados do IBGE. Pede poucos municípios por vez (URLs curtas, resposta leve)."""
    por_id: dict[str, dict] = {}
    for i in range(0, len(ids), lote):
        parte = ids[i:i + lote]
        # Os colchetes fazem parte da sintaxe da API do IBGE: localidades=N6[id1,id2]
        resposta = _get_json(f"{url}?localidades=N6[{','.join(parte)}]", sessao=sessao, tentativas=4)
        if not isinstance(resposta, list) or not resposta:
            raise RuntimeError(f"Resposta inesperada do IBGE: {str(resposta)[:200]}")
        for variavel in resposta:
            for resultado in variavel.get("resultados", []):
                for serie in resultado.get("series", []):
                    codigo = str(serie["localidade"]["id"])
                    por_id[codigo] = {
                        "id": variavel.get("id"), "variavel": variavel.get("variavel"),
                        "unidade": variavel.get("unidade"),
                        "resultados": [{"classificacoes": resultado.get("classificacoes", []), "series": [serie]}],
                    }
    faltando = [i for i in ids if i not in por_id]
    if faltando:
        raise RuntimeError(f"O IBGE não devolveu população para: {faltando}")
    return por_id


def _ibge_sidra(ids: list[str], sessao=None) -> dict[str, dict]:
    """Plano B: API SIDRA. A 1ª linha da resposta é o cabeçalho (diz o que cada campo significa);
    as demais são os valores. Devolvemos no MESMO formato da API de agregados."""
    resposta = _get_json(IBGE_SIDRA_URL.format(ids=",".join(ids)), sessao=sessao, tentativas=4)
    if not isinstance(resposta, list) or len(resposta) < 2 or not isinstance(resposta[0], dict):
        raise RuntimeError(f"Resposta inesperada do IBGE SIDRA: {str(resposta)[:200]}")
    rotulos = {rotulo: campo for campo, rotulo in resposta[0].items()}
    try:
        c_codigo, c_nome, c_valor = rotulos["Município (Código)"], rotulos["Município"], rotulos["Valor"]
    except KeyError as erro:
        raise RuntimeError(f"Cabeçalho inesperado no IBGE SIDRA: {resposta[0]}") from erro
    por_id: dict[str, dict] = {}
    for linha in resposta[1:]:
        valor = str(linha.get(c_valor, "")).strip()
        if not valor.isdigit():  # "-", "..." etc.: sem dado
            continue
        codigo = str(linha[c_codigo])
        serie = {"localidade": {"id": codigo, "nome": linha.get(c_nome)}, "serie": {"2022": valor}}
        por_id[codigo] = {"id": "93", "variavel": "População residente", "unidade": "Pessoas",
                          "resultados": [{"classificacoes": [], "series": [serie]}]}
    faltando = [i for i in ids if i not in por_id]
    if faltando:
        raise RuntimeError(f"O IBGE SIDRA não devolveu população para: {faltando}")
    return por_id


def coletar_ibge(municipios: list[dict], url: str = IBGE_URL, sessao=None) -> list[dict]:
    """Uma linha por município, com a parte da resposta do IBGE que fala dele.
    Tenta a API de agregados; se ela falhar, usa a API SIDRA (mesma tabela do Censo 2022)."""
    ids = [m["codigo_ibge"] for m in municipios]
    fonte = FONTE_IBGE
    try:
        por_id = _ibge_agregados(ids, url, sessao)
    except RuntimeError as erro_agregados:
        try:
            por_id = _ibge_sidra(ids, sessao)
            fonte = FONTE_IBGE_SIDRA
        except RuntimeError as erro_sidra:
            raise RuntimeError(f"IBGE indisponível. API de agregados: {erro_agregados} | SIDRA: {erro_sidra}") from erro_sidra

    agora = agora_iso()
    return [{"codigo_ibge": i, "coletado_em": agora, "fonte": fonte, "resposta": [por_id[i]]} for i in ids]


# ----------------------------------------------------------------------------------------------
# Fonte 3: inscrições de cidadãos (SIMULADAS)
# ----------------------------------------------------------------------------------------------
_NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Hugo", "Isabela", "João",
          "Karina", "Leandro", "Marina", "Nicolas", "Olívia", "Paulo", "Queila", "Rafael", "Sabrina", "Tiago"]
_SOBRENOMES = ["Almeida", "Barbosa", "Cardoso", "Dias", "Espíndola", "Ferreira", "Gomes", "Henrique",
               "Ibrahim", "Junqueira", "Klein", "Lopes", "Moura", "Nogueira", "Oliveira", "Pereira",
               "Queiroz", "Ribeiro", "Santos", "Teixeira"]
_CANAIS = ["whatsapp", "sms", "app"]


def _sem_acento(txt: str) -> str:
    for a, b in zip("áàâãäéèêëíìîïóòôõöúùûüç", "aaaaaeeeeiiiiooooouuuuc"):
        txt = txt.replace(a, b).replace(a.upper(), b.upper())
    return txt


def gerar_inscricoes(data_ref: date, municipios: list[dict], quantidade: int | None = None) -> list[dict]:
    """Gera inscrições FICTÍCIAS do dia. A semente é a data: rodar de novo gera o mesmo arquivo.

    Cerca de 4% dos registros saem com defeito de propósito (e-mail sem @ ou telefone curto),
    para a Prata mostrar as regras de qualidade barrando dados ruins."""
    rnd = random.Random(f"inscricoes-{data_ref.isoformat()}")
    total = quantidade or rnd.randint(40, 70)
    pesos = [5 if m["codigo_ibge"] == "3550308" else 1 for m in municipios]  # capital tem mais gente
    registros = []
    for i in range(1, total + 1):
        nome, sobrenome = rnd.choice(_NOMES), rnd.choice(_SOBRENOMES)
        municipio = rnd.choices(municipios, weights=pesos, k=1)[0]
        email = f"{_sem_acento(nome).lower()}.{_sem_acento(sobrenome).lower()}{rnd.randint(1, 999)}@example.com"
        telefone = "119" + "".join(str(rnd.randint(0, 9)) for _ in range(8))
        if rnd.random() < 0.04:
            email = email.replace("@", "")
        if rnd.random() < 0.04:
            telefone = telefone[:7]
        registros.append({
            "id_inscricao": f"INS-{data_ref:%Y%m%d}-{i:03d}",
            "nome": f"{nome} {sobrenome}", "email": email, "telefone": telefone,
            "codigo_ibge": municipio["codigo_ibge"], "canal": rnd.choice(_CANAIS),
            "data_inscricao": data_ref.isoformat(), "coletado_em": agora_iso(),
            "fonte": "simulado (dados ficticios gerados por codigo)",
        })
    return registros
