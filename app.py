# app.py
from extract import extrair_dados, carregar_dados
from cleaning import transformar
from load import carregar_staging, anos_ja_carregados

def pipeline():
    print("[EXTRACT] Baixando dados...")
    #pipeline - cada etapa é feita por ano, para não sobrecarregar o sistema
    for ano in extrair_dados(2000, 2025):
        print("[TRANSFORM & LOAD] Processando...")
        anos_existentes = anos_ja_carregados()
        
        for df in carregar_dados():
            print("[CLEANING] Iniciando a transformação dos dados...")
            df = cleaning(df)
            print("[CLEANING] Dados transformados")
            carregar_staging(df, anos_existentes)
            print("[LOAD] Dados carregados")

    print("Pipeline concluído.")

if __name__ == "__main__":
    pipeline()