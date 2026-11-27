"""
Módulo compartilhado: conexão com o Postgres da Aiven e leitura da API
aberta do Open-Meteo (temperatura máxima diária de capitais brasileiras).
Usado por app.py (e por producer.py/consumer.py, se Kafka for usado).

O nome do arquivo foi mantido (bcb_client.py) só para não quebrar os
imports dos outros scripts; o conteúdo agora é do tema CLIMA.
"""

import os
import datetime
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
import requests

DB_URL = os.environ.get("AIVEN_DB_URL")

# Cidades monitoradas. A chave (1..5) é só um id interno e reaproveita a
# coluna series_code da tabela. Dados: Open-Meteo (aberto, sem chave de API).
# Documentação: https://open-meteo.com/en/docs
SERIES = {
    6: {"name": "Bahia (Salvador)", "unit": "°C","casas": 1, "lat": -12.97, "lon": -38.51},
    1: {"name": "São Paulo",      "unit": "°C", "casas": 1, "lat": -23.55, "lon": -46.63},
    2: {"name": "Rio de Janeiro", "unit": "°C", "casas": 1, "lat": -22.91, "lon": -43.17},
    3: {"name": "Brasília",       "unit": "°C", "casas": 1, "lat": -15.79, "lon": -47.88},
    4: {"name": "Porto Alegre",   "unit": "°C", "casas": 1, "lat": -30.03, "lon": -51.23},
    5: {"name": "Manaus",         "unit": "°C", "casas": 1, "lat": -3.12,  "lon": -60.02},
}
POINTS_PER_SERIES = 30

# Pool de conexões: a Aiven fica em outra região/rede que o Render, então
# abrir uma conexão nova (TCP + handshake SSL) custa ~1s toda vez. Uma
# única página do dashboard faz várias consultas (visita, cache de cada
# série, contagem de acessos...), e abrir uma conexão nova pra cada uma
# multiplicava esse ~1s por 7-8, deixando a página bem lenta. Com o pool,
# as conexões são abertas uma vez e reaproveitadas entre requisições.
_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        if not DB_URL:
            raise RuntimeError("AIVEN_DB_URL não configurada")
        _pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DB_URL,
            sslmode="require",
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
    return _pool


@contextmanager
def get_connection():
    """Pega uma conexão emprestada do pool e devolve ao sair do bloco
    `with` — em vez de abrir/fechar uma conexão física nova toda vez."""
    conn = _get_pool().getconn()
    try:
        yield conn
    except Exception:
        # Se algo deu errado no meio de uma transação, desfaz antes de
        # devolver ao pool — senão a próxima requisição que reaproveitar
        # essa conexão herda uma transação "abortada" e falha em cascata.
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def init_db():
    """Cria as tabelas caso ainda não existam.

    Em produção, vários workers do gunicorn chamam esta função quase ao
    mesmo tempo na inicialização. `CREATE TABLE IF NOT EXISTS` não é
    100% atômico entre transações concorrentes no Postgres, então dois
    workers podem colidir tentando criar a mesma tabela ao mesmo tempo
    (erro de "duplicate key" no catálogo interno pg_type). Isso é
    inofensivo — só significa que outro worker já criou a tabela — então
    apenas ignoramos esse erro específico em vez de deixar o worker
    inteiro cair.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bcb_series (
                        series_code INTEGER NOT NULL,
                        ref_date    DATE NOT NULL,
                        value       NUMERIC NOT NULL,
                        fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (series_code, ref_date)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS visits (
                        id SERIAL PRIMARY KEY,
                        path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
            conn.commit()
    except psycopg2.errors.DuplicateTable:
        # Outro worker criou a tabela entre nossa checagem e nosso CREATE.
        # Tudo bem, o objetivo (tabela existir) já foi alcançado.
        pass
    except psycopg2.errors.UniqueViolation as exc:
        # Mesma corrida, mas manifestada como conflito no catálogo interno
        # pg_type em vez de "table already exists". Também inofensivo.
        if "pg_type" not in str(exc):
            raise


def fetch_series(series_code: int, n: int = POINTS_PER_SERIES):
    """Busca a temperatura máxima diária dos últimos N dias de uma cidade
    na API do Open-Meteo e devolve [{"ref_date": date, "value": float}]."""
    meta = SERIES[series_code]
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": meta["lat"],
        "longitude": meta["lon"],
        "daily": "temperature_2m_max",
        "past_days": max(n - 1, 0),
        "forecast_days": 1,
        "timezone": "America/Sao_Paulo",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    daily = resp.json()["daily"]  # {"time": ["2026-11-01", ...], "temperature_2m_max": [28.1, ...]}

    parsed = []
    for day, temp in zip(daily["time"], daily["temperature_2m_max"]):
        if temp is None:  # a API pode devolver null em dias sem dado
            continue
        parsed.append({"ref_date": datetime.date.fromisoformat(day), "value": float(temp)})
    return parsed[-n:]


# Alias: app.py, producer.py e consumer.py ainda chamam este nome.
fetch_series_from_bcb = fetch_series


def upsert_series(series_code: int, points: list):
    """Grava (ou atualiza) pontos de uma série direto no Postgres —
    usado tanto pelo cache do app.py quanto pelo consumer.py do Kafka."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in points:
                cur.execute(
                    """
                    INSERT INTO bcb_series (series_code, ref_date, value, fetched_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (series_code, ref_date)
                    DO UPDATE SET value = EXCLUDED.value, fetched_at = now();
                    """,
                    (series_code, p["ref_date"], p["value"]),
                )
        conn.commit()
