-- Create schema

CREATE SCHEMA IF NOT EXISTS financial_data;



-- Set search path

SET search_path TO financial_data, public;

CREATE TABLE financial_data.amount_total_dim (
    id BIGSERIAL PRIMARY KEY,
    total_amount TEXT NOT NULL UNIQUE,
    discount_amount TEXT NOT NULL UNIQUE,
    product_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL UNIQUE,
    tax_amount TEXT NOT NULL UNIQUE,
    sales_date TEXT NOT NULL UNIQUE,
    store_id TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL UNIQUE,
    quantity TEXT NOT NULL UNIQUE,
    unit_price TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE financial_data.amount_total_dim IS 'Auto-generated table from semantic analysis';

COMMENT ON COLUMN financial_data.amount_total_dim.total_amount IS 'Semantic type: :currency';

COMMENT ON COLUMN financial_data.amount_total_dim.discount_amount IS 'Semantic type: :currency';

COMMENT ON COLUMN financial_data.amount_total_dim.order_id IS 'Semantic type: :currency';

COMMENT ON COLUMN financial_data.amount_total_dim.tax_amount IS 'Semantic type: :currency';

COMMENT ON COLUMN financial_data.amount_total_dim.sales_date IS 'Semantic type: :date';

COMMENT ON COLUMN financial_data.amount_total_dim.quantity IS 'Semantic type: :currency';

COMMENT ON COLUMN financial_data.amount_total_dim.unit_price IS 'Semantic type: :currency';

-- Maintenance function for SCD Type 2 updates

CREATE OR REPLACE FUNCTION financial_data.update_dimension_scd()

RETURNS TRIGGER AS $$

BEGIN

    -- Close current record

    UPDATE 

financial_data

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