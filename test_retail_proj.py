import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders , join_orders_customers
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


def test_filter_closed_orders(spark):
    orders_df=read_orders(spark,"LOCAL")
    filtered_count_orders=filter_closed_orders(orders_df).count()
    assert filtered_count_orders == 7556


def test_join_orders_customers(spark):
    orders_df=read_orders(spark,"LOCAL")
    customers_df=read_customers(spark,"LOCAL")
    joined_df_count=join_orders_customers(orders_df, customers_df).count()
    assert joined_df_count == 68883

# lets check for get_app_config
def test_get_app_config():
    app_conf=get_app_config("LOCAL")
    assert app_conf["orders.file.path"] == "data/orders.csv"


def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

def test_count_orders_state(spark):
    customers_df=read_customers(spark,"LOCAL")
    customers_df_statewise_cnt=count_orders_state(customers_df)
    actual_result=customers_df_statewise_cnt.collect()
    expected_result= ""
    assert actual_result == expected_result









