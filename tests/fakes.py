"""Respostas FALSAS das APIs, no formato descrito na documentação (Open-Meteo e IBGE agregados).
Servem só para testar o código sem internet. Os números são inventados."""
from __future__ import annotations

from datetime import date


def falso_get_json(url, params=None, tentativas=3, sessao=None):
    if "open-meteo" in url:
        dia = params["start_date"]
        horas = [f"{dia}T{h:02d}:00" for h in range(24)]
        # chuva "sobe" com a latitude só para os municípios terem valores diferentes
        base = abs(params["latitude"] + 23.0) * 4
        chuva = [round(base if 14 <= h <= 17 else 0.0, 1) for h in range(24)]
        temp = [round(18 + h / 2, 1) for h in range(24)]
        return {
            "latitude": params["latitude"], "longitude": params["longitude"],
            "generationtime_ms": 0.5, "utc_offset_seconds": -10800,
            "timezone": "America/Sao_Paulo", "timezone_abbreviation": "-03", "elevation": 760.0,
            "hourly_units": {"time": "iso8601", "temperature_2m": "°C", "precipitation": "mm"},
            "hourly": {"time": horas, "temperature_2m": temp, "precipitation": chuva},
        }
    if "servicodados.ibge.gov.br" in url:
        ids = url.split("N6[")[1].split("]")[0].split(",")
        series = [
            {"localidade": {"id": i, "nome": f"Cidade {i[-3:]} - SP"}, "serie": {"2022": str(100000 + int(i[-3:]) * 1000)}}
            for i in ids
        ]
        return [{"id": "93", "variavel": "População residente", "unidade": "Pessoas",
                 "resultados": [{"classificacoes": [], "series": series}]}]
    raise AssertionError(f"URL inesperada no teste: {url}")


DIA_TESTE = date(2026, 11, 10)
