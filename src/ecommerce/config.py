import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    group_id: str
    topics: tuple[str, ...]


@dataclass(frozen=True)
class DwhConfig:
    db: str
    user: str
    password: str
    host: str
    port: int


@dataclass(frozen=True)
class AppConfig:
    kafka: KafkaConfig
    dwh: DwhConfig


def _get_required_env(name: str) -> str:
    value = os.getenv(name)

    if value is None or not value.strip():
        raise RuntimeError(f'Environment variable {name} is not configured')

    return value


def load_config() -> AppConfig:
    kafka_port = _get_required_env('KAFKA_HOST_PORT')
    
    kafka = KafkaConfig(
        bootstrap_servers=f'localhost:{kafka_port}',
        group_id='ecommerce-dwh-ingestion',
        topics=(
            'ecommerce.public.customers',
            'ecommerce.public.products',
            'ecommerce.public.orders',
            'ecommerce.public.order_items',
            'ecommerce.public.payments',
        )
    )

    dwh = DwhConfig(
        db=_get_required_env('POSTGRES_DWH_DB'),
        user=_get_required_env('POSTGRES_DWH_USER'),
        password=_get_required_env('POSTGRES_DWH_PASS'),
        host='localhost',
        port=int(_get_required_env('POSTGRES_DWH_HOST_PORT'))
    )

    return AppConfig(
        kafka=kafka,
        dwh=dwh
    )
