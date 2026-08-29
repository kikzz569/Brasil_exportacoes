"""Padroniza as mensagens simples de progresso da pipeline de ingestão."""

from datetime import datetime


# Exibe uma mensagem de progresso com horário, nível e etapa da ingestão.
def registrar_progresso(etapa, mensagem, nivel="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{nivel}] [{etapa}] {mensagem}")
