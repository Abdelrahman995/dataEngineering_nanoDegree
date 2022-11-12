import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
customer_dim_drop = "DROP TABLE IF EXISTS public.customer_dim"
order_payments_dim_drop = "DROP TABLE IF EXISTS public.order_payments_dim"
order_review_dim_drop = "DROP TABLE IF EXISTS public.order_review_dim"
order_date_key_dim_drop = "DROP TABLE IF EXISTS public.order_date_key_dim"
products_dim_drop = "DROP TABLE IF EXISTS public.products_dim"
seller_dim_drop = "DROP TABLE IF EXISTS public.seller"
order_item_dim_drop = "DROP TABLE IF EXISTS public.order_item"
orders_trx_fact_drop = "DROP TABLE IF EXISTS public.orders_trx_fact"

# CREATE TABLES
customer_dim_create= ("""
    CREATE TABLE IF NOT EXISTS public.customer_dim
    (
        customer_key          INTEGER PRIMARY KEY SORTKEY,
        customer_id            INTEGER,
        customer_unique_id      INTEGER,
        customer_zip_code_prefix          INTEGER,
        customer_city TEXT,
        customer_state       TEXT
    );
""")

order_payments_dim_create = ("""
    CREATE TABLE IF NOT EXISTS public.order_payments_dim
    (
        payment_key             INTEGER PRIMARY KEY SORTKEY,
        order_id               INTEGER,
        payment_sequentials            INTEGER,
        payment_type                TEXT,
        payment_installments           INTEGER,
        payment_value         FLOAT4
    );
""")

order_review_dim_create = ("""
    CREATE TABLE IF NOT EXISTS public.order_review_dim
    (
        review_key    INTEGER PRIMARY KEY,
        review_id     TIMESTAMP NOT NULL,
        order_id        TEXT NOT NULL,
        review_creation_date          TEXT,
        review_answer_timestamp        TIMESTAMP
    );
""")

order_date_key_dim_create = ("""
    CREATE TABLE IF NOT EXISTS public.order_date_key_dim
    (
        date_key     INTEGER PRIMARY KEY SORTKEY,
        order_purchase_timestamp  TEXT,
        order_approved_at   TEXT,
        order_delivered_carrier_date      TEXT,
        order_delivered_customer_date      TEXT,
        order_estimated_delivery_date       TEXT
    );
""")

products_dim_create = ("""
    CREATE TABLE IF NOT EXISTS public.products_dim
    (
        product_key     INTEGER PRIMARY KEY SORTKEY,
        product_id       TEXT,
        product_category_name   TEXT,
        product_category_name_eng        TEXT,
        product_name_lenght        INTEGER,
        product_description_lenght        INTEGER,
        product_photos_qty        INTEGER,
        product_weight_g        FLOAT4,
        product_length_cm        FLOAT4,
        product_height_cm        FLOAT4,
        product_width_cm        FLOAT4
    );
""")

seller_dim_create = ("""
    CREATE TABLE IF NOT EXISTS public.seller_dim
    (
        seller_key  INTEGER PRIMARY KEY SORTKEY,
        seller_id        INTEGER,
        seller_zip_code_prefix         TEXT,
        seller_city        TEXT,
        seller_state       TEXT
    );
""")



order_item_dim_create = ("""
    CREATE TABLE IF NOT EXISTS public.order_item_dim
    (
        order_item_key  INTEGER PRIMARY KEY SORTKEY,
        order_id        INTEGER,
        order_item_id         INTEGER,
        product_key        INTEGER,
        seller_key       INTEGER
    );
""")


orders_trx_fact_create = ("""
    CREATE TABLE IF NOT EXISTS public.orders_trx_fact
    (
        trx_key  INTEGER PRIMARY KEY SORTKEY,
        customer_key        INTEGER,
        payment_key         INTEGER,
        review_key        INTEGER,
        product_key       INTEGER,
        seller_key        INTEGER ,
        order_item_key        INTEGER ,
        date_key        INTEGER ,
        price        INTEGER DISTKEY,
        shipping_limit_date        TIMESTAMP ,
        review_score        FLOAT4 ,
        status        TEXT 
    );
""")

# QUERY LISTS

drop_table_queries = [customer_dim_drop, order_payments_dim_drop, order_review_dim_drop, order_date_key_dim_drop, 
products_dim_drop, seller_dim_drop, order_item_dim_drop, orders_trx_fact_drop]

create_table_queries = [customer_dim_create, order_payments_dim_create, order_review_dim_create, order_date_key_dim_create, 
products_dim_create, seller_dim_create, order_item_dim_create, orders_trx_fact_create]
