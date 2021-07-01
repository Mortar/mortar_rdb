import os
from time import sleep

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


@pytest.fixture(autouse=True)
def ensure_db_started():
    url = os.environ.get('DB_URL')
    if url:
        engine = create_engine(url)
        for i in range(10):
            try:
                engine.connect()
            except OperationalError:  # pragma: no cover
                sleep(5)
            else:
                break

