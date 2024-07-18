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
    '''
    A class for cleaning data.

    Parameters:
    -----------

    Attributes:
    -----------

    Methods:
    --------
    def clean_user_data(self, user_df):j

    '''

    def __init__(self):

        # self.country_lists = self.load_country_data()
        # self.country_data = self.country_lists[0]
        # self.countries = self.country_lists[1]
        # self.country_codes = self.country_lists[2]
        continents = ['America', 'North America', 'South America', 'Europe', 'Asia', 'Africa', 'Australia', 'Antarctica']
        cc_providers = ['Visa', 'JCB', 'American Express', 'Diner\'s Club', 'Maestro', 'Mastercard', 'Discovery']
        
        # Regex codes
        name_regex = r"[^A-Za-z- ]+"
        numeric_regex = r"[^0-9]+"
        null_regex = r"^(NULL|Null|null|N/A|n/a|NaN|<NA>)$"

        # Email - RFC 2821, 2822 compliant Regex filter
        email_regex = r"^((([!#$%&'*+\-/=?^_`{|}~\w])|([!#$%&'*+\-/=?^_`{|}~\w][!#$%&'*+\-/=?^_`{|}~\.\w]{0,}[!#$%&'*+\-/=?^_`{|}~\w]))[@]\w+([-.]\w+)*\.\w+([-.]\w+)*)$"
        
        uuid_regex = r"^([A-Za-z0-9]{8})[-]([A-Za-z0-9]{4})[-]([A-Za-z0-9]{4})[-]([A-Za-z0-9]{4})[-]([A-Za-z0-9]{12})$"


    def clean_user_data(self, user_df):

        # df = df.astype({'col1': 'object', 'col2': 'int'})

        # convert Series
        # my_series = pd.to_numeric(my_series)

        # # convert column "a" of a DataFrame
        # df["a"] = pd.to_numeric(df["a"])
        # You can also use it to convert multiple columns of a DataFrame via the apply() method:

        # # convert all columns of DataFrame
        # df = df.apply(pd.to_numeric) # convert all columns of DataFrame

        # # convert just columns "a" and "b"
        # df[["a", "b"]] = df[["a", "b"]].apply(pd.to_numeric)


        # General cleaning
        user_df = self.index_clean(user_df) # Sort by and drop index column
        user_df = self.null_clean(user_df) # Drop NULLs
        user_df = self.format_clean(user_df) # Strip and lowercase all
        user_df = user_df.drop_duplicates() # Drop duplicates

        # Filter out erroneous values
        user_df = user_df[~user_df.map(lambda x: len(str(x)) == 10).all(axis=1)]
        user_df['email_address'] = user_df['email_address'].str.replace(r'[@]{2,}', '@', regex=True)
    
        # Column cleaning
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
        user_df['phone_number'] = user_df['phone_number'].str.replace(r'([^0-9]+)','',regex=True)
        
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
        cc_df['card_number'] = cc_df['card_number'].str.replace(r'([^0-9]+)','',regex=True)


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
        store_df['staff_numbers'] = store_df['staff_numbers'].str.replace(r'([^0-9]+)','',regex=True)

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
        order_df['card_number'] = order_df['card_number'].str.replace(r"([^0-9]+)",'',regex=True)

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

        # Final sweep
        event_df = event_df.dropna(how='all')
        event_df = event_df.drop_duplicates()
        event_df = event_df.reset_index(drop=True)

        return event_df

    def regex_replace(self, df, column, regex_code, replace_value):
        '''
        Replaces dataframe column values matching regex expression with
        replacement value.
        '''
        regex_code = re.compile(regex_code)
        df[column] = df[column].str.replace(regex_code, replace_value, regex=False)

        return df

    def regex_none(self, df, column, regex_code):
        '''
        Sets dataframe column values matching regex expression to None.
        '''
        regex_code = re.compile(regex_code)
        mask = df[column].str.fullmatch(regex_code)
        df.loc[~mask, column] = None

        return df

    # TO DO: DROP ROW IF COLUMN NOT FIT REGEX
    def regex_drop(self, df, column, regex_code):  
        '''
        Removes dataframe rows where column values do not match
        regex expression.

        Note: This method removes all row values based on
        value from individual column.
        '''
        regex_code = re.compile(regex_code)
        df[column] = df[column].str.fullmatch(regex_code)
        df = df.loc[~df[column].isna()]

        return df

    def convert_product_weights(self, product_df):
        '''
        Extracts measurement units from column values and converts 
        said values to KG units using 'units' reference dictionary.
        '''
        units = {'kg': 1, 'g': .001, 'ml': .001, 'oz': 0.02834952}
        product_scales = product_df['weight'].str.extract(r"(\d\.?\d*+)\s*(\D+)")
        product_df['weight'] = product_scales[0].astype('float').mul(product_scales[1].map(units))

        return product_df

    def index_clean(self, df):
        '''
        Checks columns for index column. Sorts based on index value.
        After sorting, deletes existing index column and resets index.
        '''
        df['index'] = df['index'].astype('int64')
        df = df.sort_values('index')
        df = df.drop('index', axis=1)
        df = df.reset_index(drop=True)

        return df
    
    def strip_and_lower(self, df):
        ''' 
        Strips leading and trailing spaces and sets lowercase to 
        all string values in dataframe.
        '''
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.map(lambda x: x.lower() if isinstance(x, str) else x) 

        return df

    def title_clean(self, df, column):
        df[column] = df[column].astype({column : 'string'}) # String datatype
        df[column] = df[column].str.title()

        return df

    def date_clean(self, df, column):
        '''
        Parses dataframe string column for dates and converts to
        Pandas datetime datatype.
        '''
        df[column] = df[column].astype('string')
        df[column] = df[column].apply(parse)
        df[column] = pd.to_datetime(df[column], errors='coerce')
        
        return df

    def merge_columns(self, df, year_col, month_col, day_col, time_col):
        '''
        Creates new dataframe column by merging selected columns.
        Merged columns are removed.
        '''
        df['date'] = df[[year_col, month_col, day_col, time_col]].agg(' '.join, axis=1)
        df = df.drop(columns=[year_col, month_col, day_col, time_col])
        df = self.date_clean(df, 'date') # TO REMOVE???
        return df
    
    def phone_clean(self, df):
        try:
        # Country code clean
        # Remove (0), and +44(0)
        # Remove none numerica characters
            return df
        except:
            print('phone_clean error')

####################################################################
    def yaml_data_extract(self, file_path):
        '''
        Safe loads yaml file into dataframe.
        '''
        with open(file_path, 'r') as yaml_file:
            yaml_data = yaml.safe_load(yaml_file)
        
        return yaml_data

    def nested_values_list(self, dictionary, nested_value_key):
        '''
        Creates list from specified nested values.
        '''
        values_list = []
        dictionary_keys = list(dictionary.keys())
        for key in dictionary_keys:
            values_list.append(dictionary[key][nested_value_key])

        return values_list
######################################################################
   
        # COUNTRY CODES
    def cross_column_insert(self, df, column1, column2, data_dict, nested_value_key):
        '''
        Uses column1 values as dictionary keys to select values to
        insert into column2.
        '''
        unique_values = df[column1].unique()
        code_dict = {}
        for value in unique_values:
            code_dict[value] = data_dict[value][nested_value_key]
        df[column2] = df[column1].replace(code_dict)

        return df

    def category_clean(self, df, column, reference_values):
        '''
        Uses fuzzy_cat_clean() method to replaces columns values 
        with spellchecked values.

        For use with columns with a fixed number of possibly values
        (e.g. countries, country_codes, chemical elements, etc.).
        Values below spellcheck threshold are set to None.

        Uses Levenshtein Distance via FuzzyWuzzy module.
        '''
        unique_values = df[column].unique()
        value_dict = self.fuzzy_cat_clean(unique_values, reference_values)
        df[column] = df[column].replace(value_dict)

        return df

    def fuzzy_cat_clean(values, reference_values, threshold=85):
        '''
        Spellchecks values with a fixed number of possible values
        (e.g. countries, country_codes, chemical elements, etc.).

        Returns value (key) : reference_value (value) dictionary.
        Values below spellcheck threshold are set to None

        Uses Levenshtein Distance via FuzzyWuzzy module.
        '''
        corrected_values = []
        for value in values:
            try:
                value_check = process.extractOne(value, reference_values)
                if value_check[1] > threshold:
                    corrected_values.append(value_check[0])
                else:
                    corrected_values.append(None)
            except:
                print('Skipped value: ' + value)
                continue
        value_dict = dict(zip(values, corrected_values))

        return value_dict

if __name__ == '__main__':
     cln = DataCleaning()