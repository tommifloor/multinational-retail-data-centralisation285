# MRDC Data Pipeline 
import data_cleaning as dc
import data_extraction as de
import data_utils as du

# Initialize Objects
cxn = du.DatabaseConnector()
ext = de.DataExtractor()
cln = dc.DataCleaning()

cxn.init_db_engine()

# User tables
user_df = ext.read_rds_table(cxn.engine, 'legacy_users')
user_df = cln.clean_user_data(user_df)

# Credit cards
pdf_endpoint = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
cc_df = ext.retrieve_pdf_data(pdf_endpoint)
cc_df = cln.clean_card_data(cc_df)

# Stores
api_header_filepath = 'credentials/store_api_headers.yaml'
store_no_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
store_df = ext.retrieve_stores_data(api_header_filepath, store_no_endpoint, store_data_endpoint) 
store_df = cln.clean_store_data(store_df)

# Products
S3_file_path = 'products.csv' 
product_df = ext.extract_from_s3('data-handling-public','products.csv', S3_file_path)
product_df = cln.clean_products_data(product_df)

Orders
order_df = ext.read_rds_table(cxn.engine,'orders_table')
order_df = cln.clean_orders_data(order_df)

# Events
json_file_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
event_df = ext.extract_from_js(json_file_path)
event_df = cln.clean_events_data(event_df)

# Database uploads
cxn.upload_to_db(user_df, 'dim_users')
cxn.upload_to_db(cc_df, 'dim_card_details')
cxn.upload_to_db(store_df, 'dim_store_details')
cxn.upload_to_db(product_df, 'dim_products')
cxn.upload_to_db(order_df, 'orders_table')
cxn.upload_to_db(order_df, 'dim_date_times')

print("Script complete.")