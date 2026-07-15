import requests
import time
import pandas as pd
import json

# 1. Obter o link da API
url = "https://api-comexstat.mdic.gov.br/general"

#definindo os parametro para acesso a API
querystring = {"language":"pt"}
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

#definindo a função para acessar a API
def source(flow, period_from, period_to, details, metrics):
    payload = {
        "flow": flow,
        "monthDetail": True,
        "period": {
            "from": period_from,
            "to": period_to
        },
        "filters": [],
        "details": details,
        "metrics": metrics
    }

    resp = requests.post(url, json=payload, headers=headers, params=querystring)
    resp.raise_for_status()
    return resp.json()

#iniciando a requisição mês a mês para não estourar o tempo limite de 30 segundos.
resultado = []
meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']    
anos = range(2000, 2026)

#loop para iterar sobre os anos e meses.
for ano in anos:
    try:
        for mes in meses:
            for tentativas in range(10):
                try:
                    data = source('export', str(ano)+'-'+mes, str(ano)+'-'+mes, ['country', 'state', 'ncm'], ['metricFOB', 'metricKG', 'metricStatistic'])
                    resultado.append(data)
                    print(f'Mês {mes} baixado com sucesso')
                    df = pd.DataFrame(data['data']['list'])
                    print(f'DataFrame criado para o mês {mes}')
                    print(df.head())
                    break
                except requests.exceptions.HTTPError as e:
                    time.sleep(3 * (tentativas + 1))
                    if tentativas == 9:
                        print(f'Erro ao baixar mês {mes} após {tentativas+1} tentativas')
                        break
                except Exception as e:
                    print(e)
                    print(f'Erro ao baixar mês {mes}')
                    break
        print(f'Ano {ano} baixado com sucesso')
    except Exception as e:
        print(e)
        print(f'Erro ao baixar ano {ano}')
        
