"""Aplica somente a limpeza técnica necessária antes da carga no staging."""

import pandas as pd
from unidecode import unidecode

from .progress import registrar_progresso


# Renomeia as colunas da API para o padrão adotado no staging.
def coluna_snake_case(df):
    df.rename(columns={
        'coNcm': 'codigo_ncm',
        'year': 'ano',
        'monthNumber': 'mes_numero',
        'country': 'pais',
        'state': 'estado',
        'economicBlock': 'bloco_economico',
        'via': 'via',
        'urf': 'urf',
        'metricFOB': 'valor_fob',
        'metricKG': 'peso_liquido',
        'metricStatistic': 'quantidade_estatistica'
    }, inplace=True)
    return df

# Converte colunas para os tipos esperados pela tabela de staging.
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
        registrar_progresso("CLEAN", f"Erro ao tipificar os dados: {e}", "ERRO")
        raise e
    except ValueError as e:
        registrar_progresso("CLEAN", f"Erro ao tipificar os dados: {e}", "ERRO")
        raise e
    except Exception as e:
        registrar_progresso("CLEAN", f"Erro ao tipificar os dados: {e}", "ERRO")
        raise e
    return df

# Normaliza blocos sobrepostos e remove registros duplicados entre partições.
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

# Padroniza os campos textuais usados na ingestão.
def padronização_dados(df):

    colunas = [
        'pais',
        'estado',
        'ncm',
        'bloco_economico',
        'via',
        'urf'
    ]

    for coluna in colunas:
        df[coluna] = (
            df[coluna]
            .astype(str)
            .map(unidecode)
            .str.upper()
            .str.strip()
        )

    return df

# Encadeia as rotinas de limpeza técnica antes da carga.
def limpar_dados(df):
    df = coluna_snake_case(df)
    df = tipificação_dados(df)
    df = tratamento_duplicatas(df)
    df = padronização_dados(df)
    registrar_progresso("CLEAN", "Dados limpos com sucesso")
    return df
