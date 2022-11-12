## Imports
import psycopg2
import psycopg2
import configparser
import os
import pandas as pd 
from pyspark.sql import SparkSession
from helper_functions import get_number_of_nulls_in_df,drop_columns_from_df,check_duplicate_of_column,add_surrogate_key_to_df

### Reading Config File
config = configparser.ConfigParser()
config.read('config.cfg')

### Setting env virables
os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def customer_data_processing(spark,input_path,output_path):
    customers = spark.read.csv(input_path,header=True)
    customers = add_surrogate_key_to_df(customers,"customer_key")
    customers.write.parquet(output_path+'output_capstone/customers.parquet', 'overwrite')
    return customers


def order_payments_processing(spark,input_path,output_path):
    orderPayments = spark.read.csv(input_path,header=True)
    orderPayments = add_surrogate_key_to_df(orderPayments,"payment_key")
    orderPayments.write.parquet(output_path+'output_capstone/orderPayments.parquet', 'overwrite')
    return orderPayments
     
     
def order_reviews_processing(spark,input_path,output_path):
    orderReview = spark.read.csv(input_path,header=True)
    orderReview = add_surrogate_key_to_df(orderReview,"review_key")
    orderReview.write.parquet(output_path+'output_capstone/orderReview.parquet', 'overwrite')
    return orderReview
    
    
def order_date_key_processing(spark,input_path,output_path,orders):
    orders = spark.read.csv(input_path,header=True)
    columns_to_select = ["order_id","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"]	
    order_date_key_df = orders.select(columns_to_select)
    order_date_key_df = add_surrogate_key_to_df(order_date_key_df,"date_key")
    order_date_key_df.write.parquet(output_path+'output_capstone/order_date_key_df.parquet', 'overwrite')
    return orders,order_date_key_df
    
     
def products_data_processing(spark,input_path,output_path):
    products = spark.read.csv(input_path,header=True)
    products = add_surrogate_key_to_df(products,"product_key")
    products.write.parquet(output_path+'output_capstone/products.parquet', 'overwrite')
    return products
    
     
def sellers_processing(spark,input_path,output_path):
    sellers = spark.read.csv(input_path,header=True)
    sellers = add_surrogate_key_to_df(sellers,"seller_key")
    sellers.write.parquet(output_path+'output_capstone/sellers.parquet', 'overwrite')
    return sellers

    
def order_items_processing(spark,input_path,output_path,products,sellers):
    
    orderItem = spark.read.csv(input_path,header=True)
    cols_to_select  = ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price"]
    order_item_df = orderItem.select(cols_to_select)

    order_item_df = order_item_df.join(products.select("product_key","product_id"),on="product_id",how="inner")
    order_item_df = order_item_df.join(sellers.select("seller_key","seller_id"),on="seller_id",how="inner")
    order_item_df = add_surrogate_key_to_df(order_item_df,"order_item_key")
    cols_to_select =  ["order_id","order_item_id","product_key","seller_key"]
    order_item_dim_df = order_item_df.select(cols_to_select)
    order_item_dim_df.write.parquet(output_path+'output_capstone/order_item_dim_df.parquet', 'overwrite')
    return order_item_dim_df

    
def orders_trx_processing(spark,output_path,customers,orders,orderPayments,orderReview,order_date_key_df,order_item_df):
        
    orders_trx_fact_df = orders.join(customers.select("customer_id","customer_key"),on="customer_id",how="inner")\
                                .select("order_id","customer_key","order_status")
    orders_trx_fact_df = orders_trx_fact_df.join(orderPayments.select("order_id","payment_key"),on="order_id",how="inner")
    orders_trx_fact_df = orders_trx_fact_df.join(orderReview.select("order_id","review_key","review_score"),on="order_id",how="inner")
    orders_trx_fact_df = orders_trx_fact_df.join(order_date_key_df.select("order_id","date_key"),on="order_id",how="inner")
    orders_trx_fact_df = orders_trx_fact_df.join(order_item_df.select("order_id","order_item_key","product_key","seller_key","price","shipping_limit_date"),on="order_id",how="inner")
    orders_trx_fact_df = add_surrogate_key_to_df(orders_trx_fact_df,"trx_key")

    cols_to_select = ["trx_key","customer_key","payment_key","review_key","product_key","seller_key","order_item_key",
                        "date_key","price","shipping_limit_date","review_score","order_status"]
                        
    orders_trx_fact_df  = orders_trx_fact_df.select(cols_to_select)   
    
    orders_trx_fact_df.write.parquet(output_path+'output_capstone/orders_trx_fact_df.parquet', 'overwrite')

    

def main():
    ## Create spark
    spark = create_spark_session()
    
    ## Output path on S3
    output_data = "s3a://capstoneprojectxxxxxxxx/"
    
    customer_data = customer_data_processing(spark, 'data//olist_customers_dataset.csv', output_data)     
    order_payments = order_payments_processing(spark, 'data//olist_order_payments_dataset.csv', output_data)    
    order_reviews = order_reviews_processing(spark, 'data//olist_order_reviews_dataset.csv', output_data)    
    orders,order_date_key_df = order_date_key_processing(spark, 'data//olist_orders_dataset.csv', output_data)    
    products = products_data_processing(spark, 'data//olist_products_dataset.csv', output_data)    
    sellers = sellers_processing(spark, 'data//olist_sellers_dataset.csv', output_data)    
    order_items = order_items_processing(spark, 'data//olist_order_items_dataset.csv', output_data,products,sellers)  
    orders_trx_processing(spark,output_data,customer_data,orders,order_payments,order_reviews,order_date_key_df,order_items)


if __name__ == "__main__":
    main()