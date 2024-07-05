-- Column modifications in dim_products

UPDATE dim_products
    SET product_price = REPLACE(product_price, '£', '')
;

ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR(14)
;

UPDATE dim_products
    SET weight_class = (CASE 
                            WHEN weight < 2 THEN 'Light'
                            WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                            WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                            WHEN weight >= 140 THEN 'Truck_Required'
                        END)
;

ALTER TABLE dim_products
    RENAME COLUMN removed TO still_available
;

UPDATE dim_products
    SET still_available = (CASE
                            WHEN still_available = 'Still_avaliable' THEN True
                            WHEN still_available = 'Removed' THEN False
                            ELSE NULL
                        END)
;
