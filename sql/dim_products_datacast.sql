-- Cast data types to dim_products

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING (product_price::FLOAT),
    ALTER COLUMN weight TYPE FLOAT USING (weight::FLOAT),
    ALTER COLUMN "EAN" TYPE VARCHAR(18),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE,
    ALTER COLUMN uuid TYPE UUID USING (UUID::UUID),
    ALTER COLUMN still_available TYPE BOOLEAN USING (still_available::BOOLEAN)
;