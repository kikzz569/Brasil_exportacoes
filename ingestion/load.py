"""Carrega os dados ingeridos exclusivamente nas tabelas de staging do PostgreSQL."""

from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import requests
import pandas as pd
import io
import csv
import time

from .progress import registrar_progresso

load_dotenv()

engine = create_engine(os.getenv("SUPABASE_DB_URL"))

# Consulta os anos já presentes no staging para garantir idempotência.
def anos_ja_carregados():
    with engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT DISTINCT ano FROM staging.comex_staging"))
            return {row[0] for row in result}
        except:
            return set()

# Atualiza no staging as tabelas auxiliares disponibilizadas pelo ComexStat.
def tabelas_auxiliares():

    tabelas = ['uf', 'cities', 'ways', 'countries']

    for tabela in tabelas:

        registrar_progresso("LOAD:AUX", f"Carregando {tabela}")

        url = f"https://api-comexstat.mdic.gov.br/tables/{tabela}"

        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=60
        )

        resp.raise_for_status()

        dados = resp.json()['data']

        if tabela == 'countries':
            df = pd.DataFrame(dados['list'])
        else:
            df = pd.DataFrame(dados)

        registrar_progresso("LOAD:AUX", f"{tabela}: {len(df):,} registros")
        registrar_progresso("LOAD:AUX", f"Colunas: {df.columns.tolist()}")

        df.to_sql(
            name=tabela,
            con=engine,
            schema='staging',
            if_exists='replace',
            index=False
        )

        registrar_progresso("LOAD:AUX", f"{tabela} carregada com sucesso")

# Carrega um ano no staging em lotes, com novas tentativas em falhas transitórias.
def carregar_staging(df, anos_existentes, chunk_size=100_000):
    ano = int(df["ano"].iloc[0])

    if ano in anos_existentes:
        registrar_progresso("LOAD", f"Ano {ano} já carregado")
        return

    colunas = ",".join(df.columns)
    sql = f"""
        COPY staging.comex_staging ({colunas})
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER ',', NULL '\\N')
    """

    total = len(df)

    for inicio in range(0, total, chunk_size):
        fim = min(inicio + chunk_size, total)

        for tentativa in range(1, 6):
            conn = None
            try:
                registrar_progresso("LOAD", f"Lote {inicio:,} a {fim:,} (tentativa {tentativa}/5)")

                conn = engine.raw_connection()
                cur = conn.cursor()

                buffer = io.StringIO()
                df.iloc[inicio:fim].to_csv(
                    buffer,
                    index=False,
                    header=False,
                    sep=",",
                    quoting=csv.QUOTE_MINIMAL,
                    na_rep="\\N"
                )
                buffer.seek(0)

                cur.copy_expert(sql, buffer)
                conn.commit()
                cur.close()
                conn.close()
                break

            except Exception as e:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                registrar_progresso("LOAD", str(e), "ERRO")
                if tentativa == 5:
                    raise
                espera = 5 * tentativa
                registrar_progresso("LOAD", f"Nova tentativa em {espera}s", "AVISO")
                time.sleep(espera)

    anos_existentes.add(ano)
    registrar_progresso("LOAD", f"Ano {ano} carregado com sucesso")
