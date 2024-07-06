-- Add primary keys
ALTER TABLE dim_users
ADD CONSTRAINT pk_users PRIMARY KEY (user_uuid)
;

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_card_details PRIMARY KEY (card_number)
;

ALTER TABLE dim_products 
ADD CONSTRAINT pk_products PRIMARY KEY (product_code)
;

ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_details PRIMARY KEY (store_code)
;

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_date_times PRIMARY KEY (date_uuid)
;