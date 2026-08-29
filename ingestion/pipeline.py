"""Orquestra exclusivamente a pipeline de ingestão dos dados do ComexStat."""

from datetime import date

from .clean import limpar_dados
from .extract import carregar_dados, extrair_dados
from .load import anos_ja_carregados, carregar_staging, tabelas_auxiliares
from .progress import registrar_progresso

ano_atual = date.today().year

tabelas_auxiliares()


# Sincroniza o staging com os arquivos brutos já persistidos no bucket.
def sincronizar_bucket(anos_existentes):
    registrar_progresso("PIPELINE", "Sincronizando staging com o que já existe no bucket")
    algum_processado = False

    for df in carregar_dados():
        if df is None or df.empty or 'year' not in df.columns:
            registrar_progresso("PIPELINE", "Arquivo do bucket sem coluna 'year' identificável; pulando", "AVISO")
            continue

        ano = int(df['year'].iloc[0])

        if ano in anos_existentes:
            registrar_progresso("PIPELINE", f"Ano {ano} já está no staging; pulando")
            continue

        registrar_progresso("PIPELINE", f"Ano {ano} encontrado no bucket e ausente no staging; processando")
        algum_processado = True

        df_limpo = limpar_dados(df)
        carregar_staging(df_limpo, anos_existentes)
        registrar_progresso("PIPELINE", f"Ano {ano} carregado com sucesso a partir do bucket")

    if not algum_processado:
        registrar_progresso("PIPELINE", "Nenhum ano pendente no bucket; staging já sincronizado")

    registrar_progresso("PIPELINE", "Sincronização com o bucket concluída")


# Executa as etapas de sincronização, extração, limpeza técnica e carga no staging.
def pipeline():
    anos_existentes = anos_ja_carregados()

    # 1. primeiro sincroniza o que já está no bucket, sem gastar chamada de API
    sincronizar_bucket(anos_existentes)

    # 2. só então extrai da API os anos que ainda faltam (nem no bucket, nem no staging)
    registrar_progresso("PIPELINE", f"Extraindo da API os anos ainda ausentes ({2016}-{ano_atual})")
    for ano, df in extrair_dados(2016, ano_atual):
        registrar_progresso("PIPELINE", f"Processando o ano {ano}")

        df = limpar_dados(df)

        registrar_progresso("PIPELINE", "Limpeza técnica concluída; iniciando carga")

        carregar_staging(df, anos_existentes)

        registrar_progresso("PIPELINE", "Dados carregados com sucesso")

    registrar_progresso("PIPELINE", "Pipeline de ingestão concluída")




if __name__ == "__main__":
    pipeline()
