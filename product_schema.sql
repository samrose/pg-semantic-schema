-- Create schema

CREATE SCHEMA IF NOT EXISTS product_catalog;



-- Set search path

SET search_path TO product_catalog, public;

CREATE TABLE product_catalog.name_brand_dim (
    id BIGSERIAL PRIMARY KEY,
    list_price TEXT NOT NULL UNIQUE,
    size TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL UNIQUE,
    category_id TEXT NOT NULL UNIQUE,
    color TEXT NOT NULL UNIQUE,
    brand_id TEXT NOT NULL UNIQUE,
    product_id TEXT NOT NULL UNIQUE,
    dimensions_inches TEXT NOT NULL UNIQUE,
    subcategory_name TEXT NOT NULL UNIQUE,
    brand_name TEXT NOT NULL UNIQUE,
    sku TEXT NOT NULL UNIQUE,
    weight_lbs TEXT NOT NULL UNIQUE,
    category_name TEXT NOT NULL UNIQUE,
    subcategory_id TEXT NOT NULL UNIQUE,
    supplier_name TEXT NOT NULL UNIQUE,
    unit_cost TEXT NOT NULL UNIQUE,
    supplier_id TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE product_catalog.name_brand_dim IS 'Auto-generated table from semantic analysis';

COMMENT ON COLUMN product_catalog.name_brand_dim.list_price IS 'Semantic type: :currency';

COMMENT ON COLUMN product_catalog.name_brand_dim.weight_lbs IS 'Semantic type: :currency';

COMMENT ON COLUMN product_catalog.name_brand_dim.unit_cost IS 'Semantic type: :currency';

-- Maintenance function for SCD Type 2 updates

CREATE OR REPLACE FUNCTION product_catalog.update_dimension_scd()

RETURNS TRIGGER AS $$

BEGIN

    -- Close current record

    UPDATE 

product_catalog

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