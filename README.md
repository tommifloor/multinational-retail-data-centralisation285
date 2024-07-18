# Multinational Retail Data Centralization Project

## Project Goal
The central goal of this project is to create unified data structure to improve ease of access, manipulation, and analysis of retail data.

## Project File Structure
The project is split into 4 Python scripts:
- `data_utils.py`: DatabaseConnector class for connecting and managing database connections.
- `data_extraction.py`: DataExtractor class for extracting data from multiple sources, including databases, PDFs, APIs, S3 Buckets, and JSON files.
- `data_cleaning.py`: DataCleaning class for cleaning source data and removing erroneous values.
- `main.py`: A central script to call and apply the DatabaseConnector, DataExtractor, and DataCleaning classes.

The project also includes an SQL scripts for assigning datatypes and a reference_data folder for use in the DataCleaning class's spellcheck features.

## Installation and Usage
To execute the project run the main.py file in your chosen Python interpreter. Additional commands using the upload_to_db method in data_utils.py may need to be executed to upload data to database.

### Python Modules Used:

    boto3
    dateutil
    fuzzywuzzy
    numpy
    pandas
    psycopg2
    re
    requests
    sqlalchemy
    tabula
    urllib
    yaml

## License information
GNU GENERAL PUBLIC LICENSE Version 3, 29 June 2007


