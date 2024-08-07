from pyspark.sql.functions import * # type: ignore

def filter_closed_orders(orders_df):
    return orders_df.filter("order_status = 'CLOSED'")
#lets make above one generic 

def filter_generic_status_orders(orders_df,status):
    return orders_df.filter("order_status = '{}'".format(status))


def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")

def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()
