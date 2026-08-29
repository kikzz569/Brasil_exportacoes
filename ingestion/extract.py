"""Extrai dados do ComexStat e persiste a camada bruta da pipeline de ingestão."""

import requests
import time
import pandas as pd
import os
import json
from datetime import datetime, timezone
from io import BytesIO
from supabase import create_client, Client
from dotenv import load_dotenv

from .progress import registrar_progresso

BUCKET_NAME = 'dados_por_ano'
FOLDER_RAW = 'raw'
FOLDER_LOGS = 'logs'

# Carrega a configuração local usada para acessar os serviços do Supabase.
load_dotenv()

# Cria o cliente usado para acessar o armazenamento do Supabase.
def conexão_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)


supabase = conexão_supabase()

# Recupera sequencialmente os arquivos Parquet existentes na camada raw.
def carregar_dados():
    arquivos = supabase.storage.from_(BUCKET_NAME).list(FOLDER_RAW)
    for file in arquivos:
        if file['name'].endswith('.parquet'):
            res = supabase.storage.from_(BUCKET_NAME).download(f'{FOLDER_RAW}/{file["name"]}')
            df = pd.read_parquet(BytesIO(res))
            yield df

# Extrai da API os anos ausentes e persiste cada resultado anual na camada raw.
def extrair_dados(ano_inicial, ano_final):
    # Define o endpoint de extração geral do ComexStat.
    url = "https://api-comexstat.mdic.gov.br/general"

    # Define idioma e cabeçalhos enviados em cada requisição.
    querystring = {"language":"pt"}

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # Executa uma consulta à API com filtros opcionais para particionamento por bloco.
    def source(flow, period_from, period_to, details, metrics, filters=None):
        payload = {
            "flow": flow,
            "monthDetail": True,
            "period": {
                "from": period_from,
                "to": period_to
            },
            "filters": filters or [],
            "details": details,
            "metrics": metrics
        }

        # Limita a requisição a 60 segundos para evitar conexões indefinidamente abertas.
        resp = requests.post(url, json=payload, headers=headers, params=querystring, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # Lista os blocos econômicos usados para reduzir o volume das consultas que falham com erro 500.
    CODIGOS_BLOCO = [
        105,  # América Central e Caribe
        107,  # América do Norte
        48,   # América do Sul
        53,   # Associação de Nações do Sudeste Asiático - ASEAN
        27,   # Comunidade Andina das Nações - CAN
        112,  # Europa
        111,  # Mercado Comum do Sul - Mercosul
        61,   # Oceania
        41,   # Oriente Médio
        22,   # União Europeia - UE
        51,   # África
        39,   # Ásia (Exclusive Oriente Médio)
    ]

    # Particiona meses volumosos por bloco, mantendo detalhes e métricas da consulta original.
    def baixar_mes_particionado_por_bloco(ano, mes, details, metrics):
        registrar_progresso("EXTRACT", f"Particionando mês {mes}/{ano} por bloco econômico (fallback de volume)", "AVISO")
        dfs_bloco = []

        for co_bloco in CODIGOS_BLOCO:
            for tentativa in range(6):
                try:
                    data = source(
                        'export',
                        f'{ano}-{mes}', f'{ano}-{mes}',
                        details, metrics,
                        filters=[{"filter": "economicBlock", "values": [co_bloco]}]
                    )
                    if data and data.get('data') and data['data'].get('list'):
                        dfs_bloco.append(pd.DataFrame(data['data']['list']))
                        registrar_progresso("EXTRACT", f'Bloco {co_bloco} baixado com sucesso ({len(data["data"]["list"])} linhas)')
                    else:
                        registrar_progresso("EXTRACT", f"Bloco {co_bloco}: resposta OK, mas sem dados nesse mês")
                    break
                except requests.exceptions.HTTPError as e:
                    status = getattr(e.response, 'status_code', None) if e.response is not None else None

                    if status == 429:
                        # Repete erros 429 conforme o Retry-After informado pela API.
                        retry_after = e.response.headers.get('Retry-After') if e.response is not None else None
                        espera = int(retry_after) if retry_after else 10 * (tentativa + 1)
                        registrar_progresso("EXTRACT", f"Bloco {co_bloco} com rate limit (429); aguardando {espera}s (tentativa {tentativa+1}/6)", "AVISO")
                        time.sleep(espera)
                        if tentativa == 5:
                            registrar_progresso("EXTRACT", f"Bloco {co_bloco} esgotou tentativas de 429; pulando", "ERRO")
                        continue

                    # Repete erros 502 quando o Cloudflare identifica a falha como transitória.
                    corpo_erro = None
                    if e.response is not None:
                        try:
                            corpo_erro = e.response.json()
                        except Exception:
                            corpo_erro = e.response.text[:300]

                    if status == 502 and isinstance(corpo_erro, dict) and corpo_erro.get('retryable'):
                        espera = corpo_erro.get('retry_after', 60 * (tentativa + 1))
                        registrar_progresso("EXTRACT", f"Bloco {co_bloco} com 502 retryable; aguardando {espera}s (tentativa {tentativa+1}/6)", "AVISO")
                        time.sleep(espera)
                        if tentativa == 5:
                            registrar_progresso("EXTRACT", f"Bloco {co_bloco} esgotou tentativas de 502; pulando", "ERRO")
                        continue

                    # Interrompe a partição atual para os demais erros HTTP, sem nova tentativa.
                    registrar_progresso("EXTRACT", f"Bloco {co_bloco} falhou (status {status}); corpo: {corpo_erro}; pulando sem retry", "ERRO")
                    break
                except Exception as e:
                    registrar_progresso("EXTRACT", f"Bloco {co_bloco} falhou com erro inesperado: {e}; pulando sem retry", "ERRO")
                    break

        if not dfs_bloco:
            registrar_progresso("EXTRACT", f"Particionamento do mês {mes} não retornou dados", "AVISO")
            return None

        registrar_progresso("EXTRACT", f"Mês {mes} recuperado por bloco econômico ({len(dfs_bloco)}/{len(CODIGOS_BLOCO)} blocos)")
        return pd.concat(dfs_bloco, ignore_index=True)

    # Mantém o diretório local previsto para a camada raw.
    OUTPUT_DIR = 'dados/raw'
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Divide a extração por mês para limitar o volume de cada consulta.
    resultado = []
    meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    anos = range(ano_inicial, ano_final+1)

    # Verifica se um artefato já existe no bucket informado.
    def arquivo_existe_no_bucket(pasta, nome_arquivo, bucket_name):
        arquivos = supabase.storage.from_(bucket_name).list(pasta)
        return any(a['name'] == nome_arquivo for a in arquivos)

    # Percorre cada ano solicitado para criar um artefato Parquet independente.
    for ano in anos:

        # Define os nomes idempotentes dos artefatos anuais.
        nome_arquivo_data = f'raw_dados_{ano}.parquet'
        nome_arquivo_log = f'log_{ano}.json'

        # Ignora anos que já possuem dados ou log no armazenamento.
        if arquivo_existe_no_bucket('raw', nome_arquivo_data, 'dados_por_ano') or \
            arquivo_existe_no_bucket('logs', nome_arquivo_log, 'logs'):
            registrar_progresso("EXTRACT", f"Ano {ano} já existe no bucket; pulando")
            continue

        dfs_do_ano = []
        try:
            # Percorre os meses do ano preservando os detalhes e métricas da consulta.
            for mes in meses:
                DETAILS = ['country', 'state', 'ncm', 'economicBlock', 'via', 'urf']
                METRICS = ['metricFOB', 'metricKG', 'metricStatistic']
                ultimo_status = None
                mes_recuperado = False

                # Tenta obter o mês completo antes de acionar o particionamento.
                for tentativas in range(10):
                    try:
                        # Consulta exportações mensais com todas as dimensões e métricas necessárias ao staging.
                        data = source(
                            'export',
                            str(ano)+'-'+mes, str(ano)+'-'+mes,
                            DETAILS,
                            METRICS)

                        # Trata respostas sem a estrutura de dados esperada como erro HTTP.
                        if not data or 'data' not in data or data.get('data') is None:
                            raise requests.exceptions.HTTPError

                        # Acumula a resposta e o DataFrame mensal para formar o arquivo anual.
                        resultado.append(data)
                        registrar_progresso("EXTRACT", f"Mês {mes} baixado com sucesso")

                        # Converte a lista retornada pela API em DataFrame.
                        mes_df = pd.DataFrame(data['data']['list'])
                        dfs_do_ano.append(mes_df)
                        registrar_progresso("EXTRACT", f"DataFrame mensal criado com {len(mes_df.columns)} colunas")

                        mes_recuperado = True
                        break
                    except requests.exceptions.HTTPError as e:
                        # Aplica a política de repetição adequada ao status HTTP recebido.
                        status = getattr(e.response, 'status_code', None) if e.response is not None else None
                        ultimo_status = status

                        if status == 429:
                            retry_after = e.response.headers.get('Retry-After') if e.response is not None else None
                            espera = int(retry_after) if retry_after else 10 * (tentativas + 1)
                            registrar_progresso("EXTRACT", f"Rate limit (429) no mês {mes}; aguardando {espera}s (tentativa {tentativas+1}/10)", "AVISO")
                            time.sleep(espera)
                        elif status == 500:
                            # Encaminha erros 500 de volume diretamente ao particionamento por bloco.
                            registrar_progresso("EXTRACT", f"Erro 500 no mês {mes}; iniciando particionamento por bloco sem retry", "AVISO")
                            break
                        else:
                            corpo_erro = None
                            if e.response is not None:
                                try:
                                    corpo_erro = e.response.json()
                                except Exception:
                                    corpo_erro = e.response.text[:500]
                            espera = 1 * (tentativas + 1)
                            registrar_progresso("EXTRACT", f"Erro HTTP no mês {mes} (status {status}); corpo: {corpo_erro}; aguardando {espera}s", "AVISO")
                            time.sleep(espera)

                    except Exception as e:
                        registrar_progresso("EXTRACT", f"Erro ao baixar mês {mes}: {e}", "ERRO")
                        break

                # Particiona por bloco somente quando a consulta mensal completa falha com status 500.
                if not mes_recuperado and ultimo_status == 500:
                    mes_df = baixar_mes_particionado_por_bloco(ano, mes, DETAILS, METRICS)
                    if mes_df is not None:
                        dfs_do_ano.append(mes_df)
                        registrar_progresso("EXTRACT", f"DataFrame mensal criado com {len(mes_df.columns)} colunas via particionamento")
            registrar_progresso("EXTRACT", f"Ano {ano} baixado com sucesso")

            data = pd.concat(dfs_do_ano, ignore_index=True)

            registrar_progresso("EXTRACT", f"DataFrame anual criado para {ano} com {len(data):,} linhas")

            # Serializa o DataFrame anual em Parquet e envia para a camada raw.
            buffer = BytesIO()
            data.to_parquet(buffer, index=False)
            buffer.seek(0)
            supabase.storage.from_('dados_por_ano').upload(
                path=f'raw/raw_dados_{ano}.parquet',
                file=buffer.getvalue(),
                file_options={"content-type": "application/octet-stream", "upsert": "true"}
            )
            registrar_progresso("EXTRACT", f"Parquet enviado para raw/raw_dados_{ano}.parquet")

            # Registra no bucket de logs os metadados da extração anual.
            data_de_operação = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            supabase.storage.from_('logs').upload(
                path=nome_arquivo_log,
                file=BytesIO(json.dumps({
                    "ano": ano,
                    "data_de_operação": data_de_operação,
                    "linhas_totais": len(data),
                    "colunas_totais": len(data.columns)
                }).encode()).getvalue(),
                file_options={"content-type": "application/json", "upsert": "true"}
            )
            registrar_progresso("EXTRACT", f"JSON enviado para logs/{nome_arquivo_log}")

            yield ano, data

        except Exception as e:
            registrar_progresso("EXTRACT", f"Erro ao baixar ano {ano}: {e}", "ERRO")
