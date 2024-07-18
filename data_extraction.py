# MRDC project
# Script for extracting data from a variety of sources, including .csv files, APIs, and AWS S3 buckets 
import boto3
import data_utils
import pandas as pd
import requests
import tabula
import yaml

# Class constructor
class DataExtractor:
    """
    A class for extracting data from a variety of sources, including
    .csv files, APIs, and AWS S3 buckets.

    Methods:
    --------
    read_rds_table(DatabaseConnector, table_name)
        Reads database table and returns Pandas dataframe
    retrieve_pdf_data(endpoint)
        Reads PDF url data and returns Pandas datafram
    read_api_headers(headers_yaml)
        Reads API headers from YAML file
    list_number_of_store(api_header_filepath, store_no_endpoint)
        Uses API to retrieve total store count
    retrieve_stores_data(api_header_filepath, store_no_endpoint, store_data_endpoint)
        Uses API to retrive and return store data as Pandas dataframe
    extract_s3(bucket, file_name, file_path)
        Connects to Amazon S3 bucket and returns Pandas dataframe
    extract_from_js(file_path)
        Creates Pandas dataframe from JSON file
    """

    def read_rds_table(self, engine, table_name):
        user_df = pd.read_sql_table(table_name, engine)
        
        return user_df

    def retrieve_pdf_data(self, endpoint):
        cc_df = tabula.read_pdf(endpoint, pages='all')
        cc_df = pd.concat(cc_df)

        return cc_df
    
    def read_api_headers(self, headers_yaml):
        with open(headers_yaml, 'r') as api_headers_file:
            api_headers = yaml.safe_load(api_headers_file)

        return api_headers

    def list_number_of_stores(self, api_header_filepath, store_no_endpoint):
        api_headers = self.read_api_headers(api_header_filepath)
        response = requests.get(store_no_endpoint, headers=api_headers)
        number_of_stores = response.json()['number_stores']

        return number_of_stores
    
    def retrieve_stores_data(self, api_header_filepath, store_no_endpoint, store_data_endpoint):
        store_list = []
        number_of_stores = self.list_number_of_stores(api_header_filepath, store_no_endpoint)
        api_headers = self.read_api_headers(api_header_filepath)
        for store in range(number_of_stores):
            response = requests.get(store_data_endpoint.format(store_number=store), headers=api_headers)
            store_list.append(pd.json_normalize(response.json()))
            store_df = pd.concat(store_list)

        return store_df
    
    def extract_from_s3(self, bucket, file_name, file_path):
        s3 = boto3.client('s3')
        s3.download_file(bucket, file_name, file_path)
        product_df = pd.read_csv(file_path)

        return product_df
    
    def extract_from_js(self, file_path):
        df = pd.read_json(file_path)

        return df
