#system knows that  the file with name conftest.py is a file containing fixtures

import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    return get_spark_session("LOCAL")