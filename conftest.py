#system knows that  the file with name conftest.py is a file containing fixtures

import pytest
from lib.Utils import get_spark_session

#the code till yield(inclusive) will run till test cases and after that 
#the part below yield i.e stop session here will work 

#setup code is == > code before yield 
#teardown == > code after yield == > releasing the resources


@pytest.fixture
def spark():
    "creates spark session , this is a docstring 😉"
    spark_session= get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    "gives the list of customer statewise count"
    results_schema='state string, count int'
    return spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("data/test_result/state_aggregate.csv")
