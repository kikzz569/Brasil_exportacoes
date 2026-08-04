import requests
import time
import pandas as pd
import os
import json
from datetime import datetime, timezone
from io import BytesIO
from supabase import create_client, Client
from dotenv import load_dotenv

BUCKET_NAME = 'dados_por_ano'
FOLDER_RAW = 'raw'
FOLDER_LOGS = 'logs'

#carregando as variáveis do arquivo .env e inicializando a conexão com o banco de dados supabase
load_dotenv()

def conexão_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)


supabase = conexão_supabase()

#função para carregar os dados brutos do bucket
def carregar_dados():
    arquivos = supabase.storage.from_(BUCKET_NAME).list(FOLDER_RAW)
    for file in arquivos:
        if file['name'].endswith('.parquet'):
            res = supabase.storage.from_(BUCKET_NAME).download(f'{FOLDER_RAW}/{file["name"]}')
            df = pd.read_parquet(BytesIO(res))
            yield df

#criando função para app.py
def extrair_dados(ano_inicial, ano_final):
    # 1. Obter o link da API
    url = "https://api-comexstat.mdic.gov.br/general"

    #definindo os parametro para acesso a API
    querystring = {"language":"pt"}

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    #definindo a função para acessar a API (filters é opcional, usado no particionamento por estado)
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

        #Fazendo a requisição para a API (timeout evita que a requisição fique aberta por mais de 60 segundos em caso de erro)
        resp = requests.post(url, json=payload, headers=headers, params=querystring, timeout=60)
        resp.raise_for_status()
        return resp.json()

    #códigos de UF (padrão IBGE, 2 dígitos) - fixos, sem depender de chamada à API
    CODIGOS_UF = [
        11, 12, 13, 14, 15, 16, 17,          # Norte
        21, 22, 23, 24, 25, 26, 27, 28, 29,  # Nordeste
        31, 32, 33, 35,                      # Sudeste
        41, 42, 43,                          # Sul
        50, 51, 52, 53,                      # Centro-Oeste
    ]

    #quando o mês inteiro dá 500 (timeout de volume - "fullResults"), particiona por UF.
    #mantém os MESMOS details, só reduz o volume por chamada. Sem retry: se uma UF falhar, pula pra próxima.
    def baixar_mes_particionado_por_uf(ano, mes, details, metrics):
        print(f'  Particionando mês {mes}/{ano} por UF (fallback de volume)')
        dfs_uf = []

        for co_uf in CODIGOS_UF:
            for tentativa in range(6):
                try:
                    data = source(
                        'export',
                        f'{ano}-{mes}', f'{ano}-{mes}',
                        details, metrics,
                        filters=[{"filter": "state", "values": [co_uf]}]
                    )
                    if data and data.get('data') and data['data'].get('list'):
                        dfs_uf.append(pd.DataFrame(data['data']['list']))
                        print(f'    UF {co_uf} baixada com sucesso')
                    break
                except requests.exceptions.HTTPError as e:
                    status = getattr(e.response, 'status_code', None) if e.response is not None else None

                    if status == 429:
                        #429 é rate limit, é esperado com várias chamadas seguidas - continua tentando
                        retry_after = e.response.headers.get('Retry-After') if e.response is not None else None
                        espera = int(retry_after) if retry_after else 10 * (tentativa + 1)
                        print(f'    UF {co_uf} rate limit (429) — aguardando {espera}s (tentativa {tentativa+1}/6)')
                        time.sleep(espera)
                        if tentativa == 9:
                            print(f'    UF {co_uf} esgotou tentativas de 429 — pulando')
                        continue

                    #demais erros (incluindo 500 pontual numa UF específica): sem retry, pula direto
                    corpo_erro = None
                    if e.response is not None:
                        try:
                            corpo_erro = e.response.json()
                        except Exception:
                            corpo_erro = e.response.text[:300]
                    print(f'    UF {co_uf} falhou (status {status}) — corpo: {corpo_erro} — pulando, sem retry')
                    break
                except Exception as e:
                    print(f'    UF {co_uf} falhou com erro inesperado: {e} — pulando, sem retry')
                    break

        if not dfs_uf:
            print(f'  Particionamento do mês {mes} não retornou nenhum dado')
            return None

        print(f'  Mês {mes} recuperado via particionamento por UF ({len(dfs_uf)}/{len(CODIGOS_UF)} UFs)')
        return pd.concat(dfs_uf, ignore_index=True)

    #configuração do local onde o arquivo será salvo.
    OUTPUT_DIR = 'dados/raw'
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    #iniciando a requisição mês a mês para não estourar o tempo limite de 30 segundos.
    resultado = []
    meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']    
    anos = range(ano_inicial, ano_final+1)

    #função para verificar se o arquivo já existe no bucket.
    def arquivo_existe_no_bucket(pasta, nome_arquivo, bucket_name):
        arquivos = supabase.storage.from_(bucket_name).list(pasta)
        return any(a['name'] == nome_arquivo for a in arquivos)

    #loop para iterar sobre os anos e meses.
    for ano in anos:    
        
        #definindo o nome dos arquivos.
        nome_arquivo_data = f'raw_dados_{ano}.parquet'
        nome_arquivo_log = f'log_{ano}.json'

        #verificando se o arquivo já existe no bucket.
        if arquivo_existe_no_bucket('raw', nome_arquivo_data, 'dados_por_ano') or \
            arquivo_existe_no_bucket('logs', nome_arquivo_log, 'logs'):
            print(f'Ano {ano} já existe no bucket, pulando')
            continue

        dfs_do_ano = []
        try:
            #iterando sobre os meses.
            for mes in meses:
                DETAILS = ['country', 'state', 'ncm', 'economicBlock', 'via', 'urf']
                METRICS = ['metricFOB', 'metricKG', 'metricStatistic']
                ultimo_status = None
                mes_recuperado = False

                #tentativas para acessar a API
                for tentativas in range(10):
                    try:
                        #definindo os paramentros para a função source.
                        #flow: export, period: ano-mes, details: país, estado, ncm, bloco econômico, via,urf, metrics: FOB, KG,Statistic.
                        data = source(
                            'export',
                            str(ano)+'-'+mes, str(ano)+'-'+mes, 
                            DETAILS,
                            METRICS)

                        #verificando se a resposta da API está vazia.
                        if not data or 'data' not in data or data.get('data') is None:
                            raise requests.exceptions.HTTPError

                        #adicionando os dados na lista resultado.
                        resultado.append(data)
                        print(f'Mês {mes} baixado com sucesso')

                        #criando dataframe para o mês
                        mes_df = pd.DataFrame(data['data']['list'])
                        dfs_do_ano.append(mes_df)
                        print(f'DataFrame criado com {len(mes_df.columns)} colunas')

                        mes_recuperado = True
                        break
                    except requests.exceptions.HTTPError as e:
                        #tentando novamente caso ocorra erro, tratando 429 de forma diferente dos demais
                        status = getattr(e.response, 'status_code', None) if e.response is not None else None
                        ultimo_status = status

                        if status == 429:
                            retry_after = e.response.headers.get('Retry-After') if e.response is not None else None
                            espera = int(retry_after) if retry_after else 10 * (tentativas + 1)
                            print(f'Rate limit (429) no mês {mes} — aguardando {espera}s (tentativa {tentativas+1})')
                            time.sleep(espera)
                        elif status == 500:
                            #500 é timeout de volume no servidor ("timeout: fullResults") - sem retry,
                            #parte direto pro particionamento por UF
                            print(f'Erro 500 no mês {mes} — partindo direto para particionamento por UF (sem retry)')
                            break
                        else:
                            corpo_erro = None
                            if e.response is not None:
                                try:
                                    corpo_erro = e.response.json()
                                except Exception:
                                    corpo_erro = e.response.text[:500]
                            espera = 1 * (tentativas + 1)
                            print(f'Erro HTTP no mês {mes} (status {status}) — corpo: {corpo_erro} — aguardando {espera}s')
                            time.sleep(espera)

                    except Exception as e:
                        print(e)
                        print(f'Erro ao baixar mês {mes}')
                        break

                #se a consulta cheia falhou com 500 (volume), particiona por UF mantendo os mesmos details
                if not mes_recuperado and ultimo_status == 500:
                    mes_df = baixar_mes_particionado_por_uf(ano, mes, DETAILS, METRICS)
                    if mes_df is not None:
                        dfs_do_ano.append(mes_df)
                        print(f'DataFrame criado com {len(mes_df.columns)} colunas (via particionamento)')
            print(f'Ano {ano} baixado com sucesso')

            data = pd.concat(dfs_do_ano, ignore_index=True)

            print(f'DataFrame criado para o ano {ano}')
            print(data.head())

            # upload direto do DataFrame para o Supabase
            buffer = BytesIO()
            data.to_parquet(buffer, index=False)
            buffer.seek(0)
            supabase.storage.from_('dados_por_ano').upload(
                path=f'raw/raw_dados_{ano}.parquet',
                file=buffer.getvalue(),
                file_options={"content-type": "application/octet-stream", "upsert": "true"}
            )
            print(f"Arquivo Parquet enviado para o Supabase em: raw/raw_dados_{ano}.parquet")

            #registrando data e horário que os dados foram armazenados
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
            print(f"Arquivo JSON enviado para o Supabase em: logs/{nome_arquivo_log}")

            yield ano, data

        except Exception as e:
            print(e)
            print(f'Erro ao baixar ano {ano}')
