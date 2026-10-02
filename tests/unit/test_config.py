import pytest

from ecommerce.config import load_config


def test_load_config(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('KAFKA_HOST_PORT', '19092')

    monkeypatch.setenv('POSTGRES_DWH_DB', 'test_dwh')
    monkeypatch.setenv('POSTGRES_DWH_USER', 'test_user')
    monkeypatch.setenv('POSTGRES_DWH_PASS', 'test_password')
    monkeypatch.setenv('POSTGRES_DWH_HOST_PORT', '15435')

    config = load_config()

    assert config.kafka.bootstrap_servers == 'localhost:19092'
    assert config.kafka.group_id == 'ecommerce-dwh-ingestion'
    assert config.kafka.topics == (
        'ecommerce.public.customers',
        'ecommerce.public.products',
        'ecommerce.public.orders',
        'ecommerce.public.order_items',
        'ecommerce.public.payments'
    )

    assert config.dwh.db == 'test_dwh'
    assert config.dwh.user == 'test_user'
    assert config.dwh.password == 'test_password'
    assert config.dwh.host == 'localhost'
    assert config.dwh.port == 15435


def test_load_config_raises_when_env_is_missing(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('KAFKA_HOST_PORT', '19092')

    monkeypatch.setenv('POSTGRES_DWH_DB', 'test_dwh')
    monkeypatch.setenv('POSTGRES_DWH_USER', 'test_user')
    monkeypatch.setenv('POSTGRES_DWH_PASS', 'test_password')
    monkeypatch.setenv('POSTGRES_DWH_HOST_PORT', '15435')

    monkeypatch.delenv(
        'POSTGRES_DWH_HOST_PORT',
        raising=False
    )

    with pytest.raises(
        RuntimeError,
        match='Environment variable POSTGRES_DWH_HOST_PORT is not configured'
    ):
        load_config()


def test_load_config_raises_when_env_is_empty(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('KAFKA_HOST_PORT', '   ')

    with pytest.raises(
        RuntimeError,
        match='Environment variable KAFKA_HOST_PORT is not configured'
    ):
        load_config()