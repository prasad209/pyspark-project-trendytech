import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders , join_orders_customers, filter_generic_status_orders
from lib.ConfigReader import get_app_config

#fixtures :  we know spark session is always required , which is part of the setup,
# # we dont need to write it in every test case ,its a part of fixture..
#when we define a test case , the fixture variable is written in ()
#  e.g def test_read_customers(spark):
# ideally fixtures should be in a separate file names as conftest.py == > exact this name should be there
#so i created a separate file for the same



#lets test the count of customers df whether its = expected

def test_read_customers(spark):
    customers_count=read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

#similarly , lets go for orders
def test_read_orders(spark):
    orders_count= read_orders(spark,"LOCAL").count()
    assert orders_count == 68884


#python -m pytest  == > this is the command to run pytests

#lets us also write test case for filter closed orders

@pytest.mark.transformation
def test_filter_closed_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count_orders=filter_closed_orders(orders_df).count()
    assert filtered_count_orders == 7556


@pytest.mark.transformation
def test_join_orders_customers(spark):
    orders_df=read_orders(spark,"LOCAL")
    customers_df=read_customers(spark,"LOCAL")
    joined_df_count=join_orders_customers(orders_df, customers_df).count()
    assert joined_df_count == 68883

# lets check for get_app_config
def test_get_app_config():
    app_conf=get_app_config("LOCAL")
    assert app_conf["orders.file.path"] == "data/orders.csv"

@pytest.mark.fast()
def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

@pytest.mark.skip("work in progress , hre need to add the csv under test results")
def test_count_orders_state(spark,expected_results):
    customers_df=read_customers(spark,"LOCAL")
    customers_df_statewise_cnt=count_orders_state(customers_df)
    actual_result=customers_df_statewise_cnt
    assert actual_result.collect() == expected_results.collect()



@pytest.mark.generic_test_cases
def test_filter_generic_closed_status_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count_orders=filter_generic_status_orders(orders_df,"CLOSED").count()
    assert filtered_count_orders == 7556


@pytest.mark.generic_test_cases
def test_filter_generic_open_status_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count_orders=filter_generic_status_orders(orders_df,"OPEN").count()
    assert filtered_count_orders == 0

@pytest.mark.generic_test_cases
def test_filter_generic_pending_status_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count_orders=filter_generic_status_orders(orders_df,"PENDING_PAYMENT").count()
    assert filtered_count_orders == 15030

@pytest.mark.generic_test_cases
def test_filter_generic_complete_status_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count_orders=filter_generic_status_orders(orders_df,"COMPLETE").count()
    assert filtered_count_orders == 22900

#but this is a lot of redundancy , we can also parameterize above generic_test_cases and create just 1 test case



@pytest.mark.parametrize(
    "status, count",
    [("CLOSED",7556),("PENDING_PAYMENT",15030), ("COMPLETE",22900) ,("OPEN",0)]
)

@pytest.mark.consolidated_test_case
def test_generic_test_case_for_all_statuses(spark,status,count):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count_orders=filter_generic_status_orders(orders_df,status).count()
    assert filtered_count_orders == count
