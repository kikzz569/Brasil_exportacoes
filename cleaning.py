import pandas as pd
from unidecode import unidecode


#transformando os nomes das colunas.
def coluna_snake_case(df):
    df.rename(columns={
        'coNcm': 'codigo_ncm',
        'year': 'ano',
        'monthNumber': 'mes_numero',
        'country': 'pais',
        'state': 'estado',
        'ncm': 'ncm',
        'economicBlock': 'bloco_economico',
        'via': 'via',
        'urf': 'urf',
        'metricFOB': 'valor_fob',
        'metricKG': 'peso_liquido',
        'metricStatistic': 'quantidade_estatistica'
    }, inplace=True)
    return df

def tipificação_dados(df):
    try:
        df['codigo_ncm'] = df['codigo_ncm'].astype(str)
        df['ano'] = df['ano'].astype(int)
        df['mes_numero'] = df['mes_numero'].astype(int)
        df['valor_fob'] = df['valor_fob'].astype(float)
        df['peso_liquido'] = df['peso_liquido'].astype(float)
        df['quantidade_estatistica'] = df['quantidade_estatistica'].astype(float)
        df['data'] = pd.to_datetime(df['ano'].astype(str) + '-' + df['mes_numero'].astype(str))
    except TypeError as e:
        print(f'Erro ao tipificar os dados: {e}')
        raise e
    except ValueError as e:
        print(f'Erro ao tipificar os dados: {e}')
        raise e
    except Exception as e:
        print(f'Erro ao tipificar os dados: {e}')
        raise e 
    return df

def tratamento_duplicatas(df):

    mapa = {
        'União Europeia - UE': 'Europa',
        'Mercado Comum do Sul - Mercosul': 'América do Sul',
        'Comunidade Andina das Nações - CAN': 'América do Sul',
        'Associação de Nações do Sudeste Asiático - ASEAN': 'Ásia (Exclusive Oriente Médio)',
    }

    df['bloco_economico'] = df['bloco_economico'].replace(mapa)

    chave = ['codigo_ncm', 'ano', 'mes_numero', 'pais', 'estado', 'urf', 'via']
    df = df.drop_duplicates(subset=chave)
    return df

def padronização_dados(df):
    df['pais'] = unidecode(df['pais']).str.upper().str.strip()
    df['estado'] = unidecode(df['estado']).str.upper().str.strip()
    df['ncm'] = unidecode(df['ncm']).str.upper().str.strip()
    df['bloco_economico'] = unidecode(df['bloco_economico']).str.upper().str.strip()
    df['via'] = unidecode(df['via']).str.upper().str.strip()
    df['urf'] = unidecode(df['urf']).str.upper().str.strip()
    return df

def cleaning(df):
    df = coluna_snake_case(df)
    df = tipificação_dados(df)
    df = tratamento_duplicatas(df)
    df = padronização_dados(df)
    return df    

    