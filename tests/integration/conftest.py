import os
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from dotenv import load_dotenv
from psycopg import Connection

load_dotenv()


@pytest.fixture
def dwh_connection() -> Iterator[Connection[Any]]:
    db = os.environ['POSTGRES_DWH_DB']
    user = os.environ['POSTGRES_DWH_USER']
    password = os.environ['POSTGRES_DWH_PASS']
    port = os.environ['POSTGRES_DWH_HOST_PORT']

    connection = psycopg.connect(
        dbname=db,
        user=user,
        password=password,
        host='localhost',
        port=port
    )

    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()
