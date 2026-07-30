# load.py
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import requests
import pandas as pd
import io
import csv

load_dotenv()

engine = create_engine(os.getenv("SUPABASE_DB_URL"))

def anos_ja_carregados():
    with engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT DISTINCT ano FROM staging.comex_staging"))
            return {row[0] for row in result}
        except:
            return set()

def tabelas_auxiliares():

    tabelas = ['uf', 'cities', 'ways', 'countries']
    
    for tabela in tabelas:
        
        url = f'https://api-comexstat.mdic.gov.br/{tabela}' 
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json()['data']['list'])
        df.to_sql(
            name=tabela,
            con=engine,
            schema='staging',
            if_exists='replace',
            index=False
        )   
    
def carregar_staging(df, anos_existentes, chunk_size=100_000):

    ano = int(df["ano"].iloc[0])

    if ano in anos_existentes:
        print(f"[LOAD] Ano {ano} já carregado.")
        return

    colunas = ",".join(df.columns)

    sql = f"""
        COPY staging.comex_staging ({colunas})
        FROM STDIN
        WITH (
            FORMAT CSV,
            DELIMITER ',',
            NULL '\\N'
        )
    """

    total = len(df)

    conn = engine.raw_connection()
    cur = conn.cursor()

    try:
        for inicio in range(0, total, chunk_size):

            fim = min(inicio + chunk_size, total)

            tentativas = 5

            for tentativa in range(1, tentativas + 1):

                try:

                    print(
                        f"[LOAD] Lote {inicio:,} → {fim:,} "
                        f"(tentativa {tentativa}/{tentativas})"
                    )

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

                    break

                except Exception as e:

                    conn.rollback()

                    print(f"[ERRO] {e}")

                    if tentativa == tentativas:
                        raise

                    espera = 5 * tentativa

                    print(f"Nova tentativa em {espera}s...")

                    time.sleep(espera)

        anos_existentes.add(ano)

        print(f"[LOAD] Ano {ano} carregado com sucesso.")

    finally:
        cur.close()
        conn.close()