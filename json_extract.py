# Script to connect and upload data to database 
import data_cleaning as dc
import data_extraction as de
import data_utils as du

ext = de.DataExtractor()
cln = dc.DataCleaning()

json_file_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
event_df = ext.extract_from_js(json_file_path)

# General cleaning
event_df = cln.null_clean(event_df) # Drop NULLs
event_df = event_df.drop_duplicates() # Drop duplicates

# Filter out erroneous values
event_df = event_df[~event_df.map(lambda x: len(str(x)) == 10).all(axis=1)]
event_df = cln.id_clean(event_df, 'date_uuid')

# Date clean
event_df['date'] = event_df['year'] + ' ' + event_df['month'] + ' ' + event_df['day'] + ' ' + event_df['timestamp']
event_df = event_df.drop(columns=['year', 'month', 'day', 'timestamp'])
event_df = cln.date_clean(event_df, 'date')

# Final sweep
event_df = event_df.dropna(how='all')
event_df = event_df.drop_duplicates()
event_df = event_df.reset_index(drop=True)