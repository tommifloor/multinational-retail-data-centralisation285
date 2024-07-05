-- Cast data types to dim_card_details
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE DATE,
    ALTER COLUMN date_payment_confirmed TYPE DATE
;