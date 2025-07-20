-- Create schema

CREATE SCHEMA IF NOT EXISTS energy_management;



-- Set search path

SET search_path TO energy_management, public;

CREATE TABLE energy_management.kwh_billing_dim (
    id BIGSERIAL PRIMARY KEY,
    tariff_code TEXT NOT NULL UNIQUE,
    temperature_avg_f TEXT NOT NULL UNIQUE,
    meter_id TEXT NOT NULL UNIQUE,
    carbon_emissions_kg TEXT NOT NULL UNIQUE,
    renewable_percentage TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL UNIQUE,
    voltage_level TEXT NOT NULL UNIQUE,
    cost_per_kwh TEXT NOT NULL UNIQUE,
    total_cost TEXT NOT NULL UNIQUE,
    billing_period_start TEXT NOT NULL UNIQUE,
    energy_source TEXT NOT NULL UNIQUE,
    grid_connection_type TEXT NOT NULL UNIQUE,
    peak_demand_kw TEXT NOT NULL UNIQUE,
    billing_period_end TEXT NOT NULL UNIQUE,
    consumption_kwh TEXT NOT NULL UNIQUE,
    property_type TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE energy_management.kwh_billing_dim IS 'Auto-generated table from semantic analysis';

COMMENT ON COLUMN energy_management.kwh_billing_dim.temperature_avg_f IS 'Semantic type: :currency';

COMMENT ON COLUMN energy_management.kwh_billing_dim.carbon_emissions_kg IS 'Semantic type: :currency';

COMMENT ON COLUMN energy_management.kwh_billing_dim.renewable_percentage IS 'Semantic type: :currency';

COMMENT ON COLUMN energy_management.kwh_billing_dim.cost_per_kwh IS 'Semantic type: :currency';

COMMENT ON COLUMN energy_management.kwh_billing_dim.total_cost IS 'Semantic type: :currency';

COMMENT ON COLUMN energy_management.kwh_billing_dim.billing_period_start IS 'Semantic type: :date';

COMMENT ON COLUMN energy_management.kwh_billing_dim.peak_demand_kw IS 'Semantic type: :currency';

COMMENT ON COLUMN energy_management.kwh_billing_dim.billing_period_end IS 'Semantic type: :date';

COMMENT ON COLUMN energy_management.kwh_billing_dim.consumption_kwh IS 'Semantic type: :currency';

-- Maintenance function for SCD Type 2 updates

CREATE OR REPLACE FUNCTION energy_management.update_dimension_scd()

RETURNS TRIGGER AS $$

BEGIN

    -- Close current record

    UPDATE 

energy_management

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