# Script to connect and upload data to database 
import data_cleaning as dc
import data_extraction as de
import data_utils as du

cxn = du.DatabaseConnector()
cxn.init_db_engine()

ext = de.DataExtractor()
cln = dc.DataCleaning()


user_df = ext.read_rds_table(cxn.engine, 'legacy_users')
user_df = cln.clean_user_data(user_df)

cc_df = ext.retrieve_pdf_data()
cc_df = cln.clean_card_data(cc_df)

store_df = ext.retrieve_stores_data()
store_df = cln.clean_store_data(store_df)

file_path = 'products.csv'
product_df = ext.extract_from_s3('data-handling-public','products.csv',file_path)
product_df = cln.clean_products_data(product_df)

order_df = ext.read_rds_table(cxn.engine,'orders_table')
order_df = cln.clean_orders_data(order_df)

