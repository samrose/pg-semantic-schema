-- Create schema

CREATE SCHEMA IF NOT EXISTS customer_data;



-- Set search path

SET search_path TO customer_data, public;

CREATE TABLE customer_data.name_customer_dim (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE,
    zip_code TEXT NOT NULL UNIQUE,
    last_name TEXT NOT NULL UNIQUE,
    customer_type TEXT NOT NULL UNIQUE,
    registration_date TEXT NOT NULL UNIQUE,
    country TEXT NOT NULL UNIQUE,
    address TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    phone TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL UNIQUE,
    city TEXT NOT NULL UNIQUE,
    birth_date TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE customer_data.name_customer_dim IS 'Auto-generated table from semantic analysis';

COMMENT ON COLUMN customer_data.name_customer_dim.zip_code IS 'Semantic type: :currency';

COMMENT ON COLUMN customer_data.name_customer_dim.registration_date IS 'Semantic type: :date';

COMMENT ON COLUMN customer_data.name_customer_dim.email IS 'Semantic type: :email';

COMMENT ON COLUMN customer_data.name_customer_dim.phone IS 'Semantic type: :phone';

COMMENT ON COLUMN customer_data.name_customer_dim.birth_date IS 'Semantic type: :date';

-- Maintenance function for SCD Type 2 updates

CREATE OR REPLACE FUNCTION customer_data.update_dimension_scd()

RETURNS TRIGGER AS $$

BEGIN

    -- Close current record

    UPDATE 

customer_data

." || TG_TABLE_NAME || "

    SET expiry_date = CURRENT_DATE, is_current = FALSE

    WHERE dim_id = NEW.dim_id AND is_current = TRUE;

    

    -- Insert new record

    NEW.effective_date := CURRENT_DATE;

    NEW.expiry_date := '9999-12-31';

    NEW.is_current := TRUE;

    

    RETURN NEW;

END;

$$ LANGUAGE plpgsql;