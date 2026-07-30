# load.py
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

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
        resp = requests.post(url, json=payload, headers=headers, params=querystring, timeout=60)
        resp.raise_for_status()
        resp = pd.DataFrame(resp.json()['data']['list'])
        resp.to_sql(
            name=tabela,
            con=engine,
            schema='staging',
            if_exists='replace',
            index=False
        )   
    
def carregar_staging(df, anos_existentes):
    ano = df['ano'].iloc[0]

    if ano in anos_existentes:
        print(f'[LOAD] Ano {ano} já carregado, pulando...')
        return

    df.to_sql(
        name='comex_staging',
        con=engine,
        schema='staging',
        if_exists='append',
        index=False
    )
    anos_existentes.add(ano)
    print(f'[LOAD] Ano {ano} → {len(df):,} linhas inseridas')