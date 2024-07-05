# Script for cleaning data from sources
import numpy as np
import re
import tabula
import yaml

import pandas as pd

from dateutil.parser import parse
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


class DataCleaning:
    """
    A class for cleaning data.

    Parameters:
    -----------

    Attributes:
    -----------

    Methods:
    --------
    def clean_user_data(self, user_df):j

    """

    def __init__(self):

        self.country_lists = self.load_country_data()
        self.country_data = self.country_lists[0]
        self.countries = self.country_lists[1]
        self.country_codes = self.country_lists[2]

    def clean_user_data(self, user_df):
        # General cleaning
        user_df = self.index_clean(user_df) # Sort by and drop index column
        user_df = self.null_clean(user_df) # Drop NULLs
        user_df = self.format_clean(user_df) # Strip and lowercase all
        user_df = user_df.drop_duplicates() # Drop duplicates

        # Filter out erroneous values
        user_df = user_df[~user_df.map(lambda x: len(str(x)) == 10).all(axis=1)]
    
        # Targeted cleaning
        user_df = self.name_clean(user_df, 'first_name')
        user_df = self.name_clean(user_df, 'last_name')
        user_df = self.title_clean(user_df, 'company')
        user_df = self.address_clean(user_df)
        user_df = self.country_clean(user_df)
        user_df = self.country_code_clean(user_df)
        user_df = self.email_clean(user_df)
        user_df = self.id_clean(user_df,'user_uuid')

        # TO DO
        #user_df = self.phone_clean(user_df)
        user_df['phone_number'] = user_df['phone_number'].astype('string')
        user_df['phone_number'] = user_df['phone_number'].str.replace(r'([^0-9]+)',"",regex=True)
        
        user_df = self.date_clean(user_df, 'date_of_birth')
        user_df = self.date_clean(user_df, 'join_date')

        # Drop row if more than X number values are empty
        # Drop row if the specific columns are empty (for example, names and userid)

        # Final sweep
        user_df = user_df.dropna(how='all')
        user_df = user_df.drop_duplicates()
        user_df = user_df.reset_index(drop=True)

        return user_df

    def clean_card_data(self, cc_df):

        # General clean
        cc_df = cc_df.reset_index(drop=True)
        cc_df = self.null_clean(cc_df)
        cc_df = self.format_clean(cc_df)

        # Erroneous values
        cc_df = cc_df[~cc_df.map(lambda x: len(str(x)) == 10).all(axis=1)]

        # Targeted clean
        cc_df['card_number'] = cc_df['card_number'].astype('string')
        cc_df['card_number'] = cc_df['card_number'].str.replace(r'([^0-9]+)',"",regex=True)


        cc_df['card_provider'] = cc_df['card_provider'].astype('string')
        cc_df = self.provider_clean(cc_df)

        # Date clean
        cc_df = self.date_clean(cc_df,'date_payment_confirmed')
        # Expiry_date to datetime64
        cc_df['expiry_date'] = pd.to_datetime(cc_df['expiry_date'], format='%m/%y', errors='coerce')

        # TO DO:
        # Card check - Luhn algorithm
        # Card providers - CC check number check

        # Final sweep
        cc_df = cc_df.dropna(how='all')
        cc_df = cc_df.drop_duplicates()
        cc_df = cc_df.reset_index(drop=True)

        return cc_df

    def clean_store_data(self, store_df):
        # General clean
        store_df = store_df.sort_values('index')
        store_df = store_df.drop('index', axis=1)
        store_df = store_df.reset_index(drop=True)
        store_df = self.null_clean(store_df) # Drop NULLs
        store_df = store_df.drop_duplicates() # Drop duplicates

        # Filter out erroneous values
        store_df = store_df[~store_df.map(lambda x: len(str(x)) == 10).all(axis=1)]

        # Targeted clean
        store_df['staff_numbers'] = store_df['staff_numbers'].astype('string')
        store_df['staff_numbers'] = store_df['staff_numbers'].str.replace(r'([^0-9]+)',"",regex=True)

        store_df = self.date_clean(store_df,'opening_date')
        store_df = self.address_clean(store_df)
        store_df = self.continent_clean(store_df)

        store_df = store_df.dropna(how='all') # Drop NULLs
        store_df = store_df.dropna(axis=1,how='all') # Drop column NULLs
        store_df = store_df.drop_duplicates() # Drop duplicates
        store_df = store_df.reset_index(drop=True)

        return store_df
    
    def clean_products_data(self, product_df):

        product_df = product_df.drop(product_df.columns[0], axis=1)
        product_df = product_df.reset_index(drop=True)
        product_df = self.null_clean(product_df) # Drop NULLs

        # Filter out erroneous values
        product_df = product_df[~product_df.map(lambda x: len(str(x)) == 10).all(axis=1)]
        product_df = product_df.drop(product_df.loc[product_df['uuid'].apply(len)<26].index)

        product_df = self.date_clean(product_df,'date_added')

        product_df = self.convert_product_weights(product_df)

        product_df = product_df.dropna(how='all') # Drop NULLs
        product_df = product_df.dropna(axis=1,how='all') # Drop column NULLs
        product_df = product_df.drop_duplicates() # Drop duplicates
        product_df = product_df.reset_index(drop=True)

        return product_df

    def clean_orders_data(self, order_df):
        # Cleaning
        order_df = order_df.drop('level_0', axis=1)
        order_df = order_df.drop('index', axis=1)
        order_df = order_df.drop('first_name', axis=1)
        order_df = order_df.drop('last_name', axis=1)
        order_df = order_df.drop('1', axis=1)

        order_df = order_df[~order_df.map(lambda x: len(str(x)) == 10).all(axis=1)]

        order_df['card_number'] = order_df['card_number'].astype('string')
        order_df['card_number'] = order_df['card_number'].str.replace(r'([^0-9]+)',"",regex=True)

        order_df = self.null_clean(order_df)
        order_df = order_df.drop_duplicates() 
        order_df = order_df.reset_index(drop=True)

        return order_df
    
    def clean_events_data(self, event_df):
        # General cleaning
        event_df = self.null_clean(event_df) # Drop NULLs
        event_df = event_df.drop_duplicates() # Drop duplicates

        # Filter out erroneous values
        event_df = event_df[~event_df.map(lambda x: len(str(x)) == 10).all(axis=1)]
        event_df = self.id_clean(event_df, 'date_uuid')

        # Date clean
        # event_df['date'] = event_df['year'] + ' ' + event_df['month'] + ' ' + event_df['day'] + ' ' + event_df['timestamp']
        # event_df = event_df.drop(columns=['year', 'month', 'day', 'timestamp'])
        # event_df = self.date_clean(event_df, 'date')

        # Final sweep
        event_df = event_df.dropna(how='all')
        event_df = event_df.drop_duplicates()
        event_df = event_df.reset_index(drop=True)

        return event_df

    def convert_product_weights(self, product_df):
        units = {'kg': 1, 'g': .001, 'ml': .001, 'oz': 0.02834952}
        product_scales = product_df['weight'].str.extract(r'(\d\.?\d*+)\s*(\D+)')
        product_df['weight'] = product_scales[0].astype('float').mul(product_scales[1].map(units))

        return product_df

    def index_clean(self, df):
        df['index'] = df['index'].astype('int64')
        df = df.sort_values('index')
        df = df.drop('index', axis=1)
        df = df.reset_index(drop=True)

        return df

    def null_clean(self, df):
        df = df.fillna(np.nan)
        df = df.replace(r"^(NULL|Null|null|N/A|n/a|NaN|<NA>)$", np.nan, regex=True)
        df = df.dropna(how='all')

        return df
    
    def format_clean(self, df):
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.map(lambda x: x.lower() if isinstance(x, str) else x) 

        return df

    def title_clean(self, df, column_name):
        df[column_name] = df[column_name].astype({column_name : 'string'}) # String datatype
        df[column_name] = df[column_name].str.title() # Title case

        return df
    
    def name_clean(self, df, column_name):
        # Remove non-alphabetic characters (except '-')
        df[column_name] = df[column_name].str.replace(r'[^A-Za-z-]+', '', regex=True)
        df = self.title_clean(df, column_name)

        return df

    def date_clean(self, df, column_name):
        df[column_name] = df[column_name].astype('string')
        df[column_name] = df[column_name].apply(parse)
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        
        return df

    def address_clean(self, df):
        # Replace escape character line breaks
        df['address'] = df['address'].str.replace('\n', ', ', regex=False)
        df = self.title_clean(df, 'address')

        return df

    def email_clean(self, df):
        # Email - RFC 2821, 2822 compliant Regex filter
        regex_filter = re.compile(r"^((([!#$%&'*+\-/=?^_`{|}~\w])|([!#$%&'*+\-/=?^_`{|}~\w][!#$%&'*+\-/=?^_`{|}~\.\w]{0,}[!#$%&'*+\-/=?^_`{|}~\w]))[@]\w+([-.]\w+)*\.\w+([-.]\w+)*)$")
        # Correct double @ symblols
        df['email_address'] = df['email_address'].str.replace(r'[@]{2,}', '@', regex=True)
        # Remove invalid email addresses
        df = self.regex_check(df,'email_address', regex_filter)

        return df
    
    def phone_clean(self, df):
        try:
        # Country code clean
        # Remove (0), and +44(0)
        # Remove none numerica characters
            return df
        except:
            print("phone_clean error")

    def load_country_data(self):
        country_codes = []
        with open('reference_data/country_data.yaml', "r") as country_file:
            country_data = yaml.safe_load(country_file)
            countries = list(country_data.keys())
            for country in countries:
                country_codes.append(country_data[country]['2-Letter Country Code'])

        return country_data, countries, country_codes

    def country_clean(self, df):
        df['country'] = df['country'].str.replace(r'[^A-Za-z-]+', '', regex=True) 
        # Fuzzy country name spellcheck
        unique_countries = list(df['country'].unique())
        corrected_countries = []
        for i, country in enumerate(unique_countries):
            country_check = process.extractOne(country, self.countries)
            if country_check[1] > 85:
                corrected_countries.append(country_check[0])
            else:
                corrected_countries.append(None)
        country_dict = dict(zip(unique_countries, corrected_countries))
        df['country'] = df['country'].replace(country_dict)
        df = self.title_clean(df,'country')

        return df
    
    def continent_clean(self, df):
        df['continent'] = df['continent'].str.replace(r'[^A-Za-z-]+', '', regex=True)
        unique_continents = list(df['continent'].unique())
        continents = ['America', 'North America', 'South America', 'Europe', 'Asia', 'Africa', 'Australia', 'Antarctica']
        corrected_continents = []
        for i, continent in enumerate(unique_continents):
            continent_check = process.extractOne(continent, continents)
            if continent_check[1] > 75:
                corrected_continents.append(continent_check[0])
            else:
                corrected_continents.append(None)
        continent_dict = dict(zip(unique_continents, corrected_continents))
        df['continent'] = df['continent'].replace(continent_dict)
        df = self.title_clean(df,'continent')

        return df
    
    # TO DO: Fix None errors / refactor
    def country_code_clean(self, df):
        df['country_code'] = df['country_code'].str.replace(r'[^A-Za-z- ]+', '', regex=True)
        unique_countries = list(df['country'].unique())
        code_dict = {}
        for country in unique_countries:
            code_dict[country] = self.country_data[country]['2-Letter Country Code']
        df['country_code'] = df['country'].replace(code_dict)

        return df
    
    def provider_clean(self, df):
        unique_providers = list(df['card_provider'].unique())
        ref_providers = ['Visa', 'JCB', 'American Express', 
                        'Diner\'s Club', 'Maestro', 
                        'Mastercard', 'Discovery'
                        ]
        provider_list = []
        for provider in unique_providers:
            provider_check = process.extractOne(provider, ref_providers)
            provider_list.append(provider_check[0])
        provider_dict = dict(zip(unique_providers, provider_list))
        df['card_provider'] = df['card_provider'].replace(provider_dict)
        
        return df
    
    def id_clean(self, df, column):  
            regex_filter = re.compile(r"^([A-Za-z0-9]{8})[-]([A-Za-z0-9]{4})[-]([A-Za-z0-9]{4})[-]([A-Za-z0-9]{4})[-]([A-Za-z0-9]{12})$")
            df = self.regex_check(df, column, regex_filter)
            df = df.loc[~df[column].isna()]

            return df

    def regex_check(self, df, column, regex_code):
        regex_filter = re.compile(regex_code)
        mask = df[column].str.fullmatch(regex_filter)
        df.loc[~mask, column] = None

        return df       

if __name__ == "__main__":
     cln = DataCleaning()