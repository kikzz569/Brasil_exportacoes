"""Valida os fluxos críticos da pipeline de ingestão sem acessar serviços externos."""

import importlib
import sys
import types
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests


class FakeResponse:
    """Representa uma resposta HTTP controlada pelos testes."""

    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self.payload = payload
        self.headers = headers or {}
        self.text = str(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        return self.payload


class FakeStorage:
    """Simula os buckets usados pela etapa de extração."""

    def __init__(self):
        self.uploads = []

    def from_(self, bucket):
        self.bucket = bucket
        return self

    def list(self, folder):
        return []

    def upload(self, **kwargs):
        self.uploads.append((self.bucket, kwargs))


class FakeSupabase:
    """Expõe somente o armazenamento necessário aos testes."""

    def __init__(self):
        self.storage = FakeStorage()


def importar_extract(fake_supabase):
    """Importa a extração com um cliente Supabase simulado."""

    modulo_supabase = types.ModuleType("supabase")
    modulo_supabase.Client = object
    modulo_supabase.create_client = Mock(return_value=fake_supabase)
    sys.modules.pop("ingestion.extract", None)
    with patch.dict(sys.modules, {"supabase": modulo_supabase}):
        return importlib.import_module("ingestion.extract")


class CleanTests(unittest.TestCase):
    """Confere a limpeza técnica aplicada antes do staging."""

    def test_limpar_dados_tipifica_padroniza_e_remove_duplicatas(self):
        from ingestion.clean import limpar_dados

        registro = {
            "coNcm": "0101",
            "year": "2024",
            "monthNumber": "1",
            "country": "México ",
            "state": "São Paulo",
            "ncm": " Animais vivos ",
            "economicBlock": "União Europeia - UE",
            "via": "Marítima",
            "urf": "Santos",
            "metricFOB": "10.5",
            "metricKG": "2.5",
            "metricStatistic": "1",
        }

        with patch("ingestion.clean.registrar_progresso"):
            resultado = limpar_dados(pd.DataFrame([registro, registro]))

        self.assertEqual(len(resultado), 1)
        self.assertEqual(resultado.iloc[0]["pais"], "MEXICO")
        self.assertEqual(resultado.iloc[0]["bloco_economico"], "EUROPA")
        self.assertEqual(resultado.iloc[0]["ano"], 2024)


class PipelineTests(unittest.TestCase):
    """Confere a sincronização idempotente entre bucket e staging."""

    def test_sincronizar_bucket_carrega_somente_ano_ausente(self):
        bruto_2022 = pd.DataFrame({"year": [2022]})
        bruto_2023 = pd.DataFrame({"year": [2023]})
        invalido = pd.DataFrame({"outra_coluna": [1]})
        limpo_2023 = pd.DataFrame({"ano": [2023]})

        modulo_extract = types.ModuleType("ingestion.extract")
        modulo_extract.carregar_dados = Mock(return_value=iter([bruto_2022, bruto_2023, invalido]))
        modulo_extract.extrair_dados = Mock()
        modulo_clean = types.ModuleType("ingestion.clean")
        modulo_clean.limpar_dados = Mock(return_value=limpo_2023)
        modulo_load = types.ModuleType("ingestion.load")
        modulo_load.anos_ja_carregados = Mock()
        modulo_load.carregar_staging = Mock()
        modulo_load.tabelas_auxiliares = Mock()
        modulo_progress = types.ModuleType("ingestion.progress")
        modulo_progress.registrar_progresso = Mock()

        falsos_modulos = {
            "ingestion.extract": modulo_extract,
            "ingestion.clean": modulo_clean,
            "ingestion.load": modulo_load,
            "ingestion.progress": modulo_progress,
        }
        sys.modules.pop("ingestion.pipeline", None)
        with patch.dict(sys.modules, falsos_modulos):
            pipeline = importlib.import_module("ingestion.pipeline")
            anos_existentes = {2022}
            pipeline.sincronizar_bucket(anos_existentes)

        modulo_clean.limpar_dados.assert_called_once_with(bruto_2023)
        modulo_load.carregar_staging.assert_called_once_with(limpo_2023, anos_existentes)
        modulo_extract.extrair_dados.assert_not_called()


class ExtractTests(unittest.TestCase):
    """Confere as políticas de retry e particionamento da extração."""

    @staticmethod
    def resposta_com_dados(payload):
        periodo = payload["period"]["from"]
        return FakeResponse(200, {"data": {"list": [{"year": 2024, "periodo": periodo}]}})

    def test_429_respeita_retry_after(self):
        fake_supabase = FakeSupabase()
        extract = importar_extract(fake_supabase)
        chamadas = 0

        def responder(*args, **kwargs):
            nonlocal chamadas
            chamadas += 1
            if chamadas == 1:
                return FakeResponse(429, {}, {"Retry-After": "0"})
            return self.resposta_com_dados(kwargs["json"])

        with patch.object(extract.requests, "post", side_effect=responder), patch.object(
            extract.time, "sleep"
        ) as sleep, patch.object(extract, "registrar_progresso"):
            resultado = list(extract.extrair_dados(2024, 2024))

        sleep.assert_called_once_with(0)
        self.assertEqual(chamadas, 13)
        self.assertEqual(resultado[0][0], 2024)
        self.assertEqual(len(fake_supabase.storage.uploads), 2)

    def test_500_particiona_sem_retry_e_502_retryable_repete(self):
        fake_supabase = FakeSupabase()
        extract = importar_extract(fake_supabase)
        primeira_consulta = True
        primeiro_bloco = True
        payloads = []

        def responder(*args, **kwargs):
            nonlocal primeira_consulta, primeiro_bloco
            payload = kwargs["json"]
            payloads.append(payload)
            if primeira_consulta and not payload["filters"]:
                primeira_consulta = False
                return FakeResponse(500, {"message": "timeout: fullResults"})
            if primeiro_bloco and payload["filters"]:
                primeiro_bloco = False
                return FakeResponse(502, {"retryable": True, "retry_after": 0})
            return self.resposta_com_dados(payload)

        with patch.object(extract.requests, "post", side_effect=responder), patch.object(
            extract.time, "sleep"
        ) as sleep, patch.object(extract, "registrar_progresso"):
            resultado = list(extract.extrair_dados(2024, 2024))

        sleep.assert_called_once_with(0)
        self.assertEqual(payloads[0]["filters"], [])
        self.assertTrue(all(payload["filters"] for payload in payloads[1:14]))
        self.assertEqual(resultado[0][0], 2024)


class FakeCursor:
    """Registra as chamadas COPY realizadas pela carga simulada."""

    def __init__(self):
        self.copias = []

    def copy_expert(self, sql, buffer):
        self.copias.append((sql, buffer.read()))

    def close(self):
        return None


class FakeConnection:
    """Simula uma conexão bruta usada pelo COPY do PostgreSQL."""

    def __init__(self):
        self.cursor_atual = FakeCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_atual

    def commit(self):
        self.commits += 1

    def close(self):
        return None


class LoadTests(unittest.TestCase):
    """Confere os lotes e as novas tentativas da carga no staging."""

    def test_carregar_staging_mantem_lotes_e_retry(self):
        engine = Mock()
        primeira_conexao = FakeConnection()
        segunda_conexao = FakeConnection()
        engine.raw_connection.side_effect = [RuntimeError("falha transitória"), primeira_conexao, segunda_conexao]
        sys.modules.pop("ingestion.load", None)

        with patch("sqlalchemy.create_engine", return_value=engine):
            load = importlib.import_module("ingestion.load")

        anos_existentes = set()
        dados = pd.DataFrame({"ano": [2024, 2024, 2024], "valor": [1, 2, 3]})
        with patch.object(load.time, "sleep") as sleep, patch.object(load, "registrar_progresso"):
            load.carregar_staging(dados, anos_existentes, chunk_size=2)

        sleep.assert_called_once_with(5)
        self.assertEqual(engine.raw_connection.call_count, 3)
        self.assertEqual(len(primeira_conexao.cursor_atual.copias), 1)
        self.assertEqual(len(segunda_conexao.cursor_atual.copias), 1)
        self.assertEqual(anos_existentes, {2024})


if __name__ == "__main__":
    unittest.main()
