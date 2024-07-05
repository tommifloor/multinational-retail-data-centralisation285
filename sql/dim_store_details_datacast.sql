-- Cast data types to dim_store_details

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING (longitude::FLOAT),
    ALTER COLUMN locality TYPE VARCHAR (255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING (staff_numbers::SMALLINT),
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN latitude TYPE FLOAT USING (latitude::FLOAT),
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR (255)
    ;