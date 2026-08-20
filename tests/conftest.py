"""Фикстуры для продуктовых тестов.

Тесты идут против ЖИВОГО Postgres из docker-compose, а не против моков:
проверяются SQL-витрины, и мок доказывал бы только то, что мок работает.

Изоляция — отдельной схемой на каждый тест. В `test_product.setup()` создаётся
схема с девятью рукописными клиентами, `search_path` пришпиливается к ней одной
(без public), и там же стоит проверка: если search_path оказался другим, тест
падает, не дотронувшись до основной базы с полумиллионом платежей.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

DSN = os.environ.get(
    "REVSCOPE_DSN",
    "postgresql://revscope:revscope@localhost:5433/revscope")


@pytest.fixture
def conn():
    """Соединение с поднятой схемой-фикстурой; схема сносится после теста.

    Стенда нет — тест пропускается, а не падает: набор должен запускаться
    на машине без docker, иначе его перестанут гонять совсем.
    """
    if psycopg is None:
        pytest.skip("psycopg не установлен")
    try:
        connection = psycopg.connect(DSN)
    except Exception as e:
        pytest.skip(f"нет Postgres по {DSN}: {type(e).__name__} — "
                    f"подними стенд командой docker compose up -d")

    # Импорт отложенный: модуль тестов к этому моменту уже загружен pytest,
    # а на уровне файла это дало бы циклический импорт.
    from tests import test_product as tp

    try:
        tp.setup(connection)
        yield connection
    finally:
        try:
            tp.teardown(connection)
        finally:
            connection.close()
