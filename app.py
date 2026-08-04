# app.py
from extract import extrair_dados
from cleaning import cleaning
from load import carregar_staging, anos_ja_carregados
from datetime import date

ano_atual = date.today().year

def pipeline():
    anos_existentes = anos_ja_carregados()

    for ano, df in extrair_dados(1997, ano_atual) or arquivos:
        print(f"Processando {ano}")

        df = cleaning(df)

        print(f"iniciando o carregamento do ano {ano}")

        carregar_staging(df, anos_existentes)

        print(f"Dados carregados")

    print("Pipeline concluído.")

if __name__ == "__main__":
    pipeline()