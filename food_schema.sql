-- Create schema

CREATE SCHEMA IF NOT EXISTS food_safety;



-- Set search path

SET search_path TO food_safety, public;

CREATE TABLE food_safety.inspection_violation_dim (
    id BIGSERIAL PRIMARY KEY,
    violation_code TEXT,
    certification_status TEXT NOT NULL UNIQUE,
    inspection_type TEXT NOT NULL UNIQUE,
    inspection_score TEXT NOT NULL UNIQUE,
    inspection_id TEXT NOT NULL UNIQUE,
    food_handler_certified TEXT NOT NULL UNIQUE,
    cleaning_schedule_followed TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL UNIQUE,
    city TEXT NOT NULL UNIQUE,
    violation_description TEXT NOT NULL UNIQUE,
    establishment_name TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    temperature_log_compliant TEXT NOT NULL UNIQUE,
    corrective_action_required TEXT NOT NULL UNIQUE,
    license_number TEXT NOT NULL UNIQUE,
    pest_control_current TEXT NOT NULL UNIQUE,
    inspector_id TEXT NOT NULL UNIQUE,
    severity_level TEXT,
    zip_code TEXT NOT NULL UNIQUE,
    inspection_date TEXT NOT NULL UNIQUE,
    establishment_type TEXT NOT NULL UNIQUE,
    phone TEXT NOT NULL UNIQUE,
    follow_up_date TEXT,
    risk_category TEXT NOT NULL UNIQUE,
    address TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE food_safety.inspection_violation_dim IS 'Auto-generated table from semantic analysis';

COMMENT ON COLUMN food_safety.inspection_violation_dim.inspection_score IS 'Semantic type: :currency';

COMMENT ON COLUMN food_safety.inspection_violation_dim.email IS 'Semantic type: :email';

COMMENT ON COLUMN food_safety.inspection_violation_dim.zip_code IS 'Semantic type: :currency';

COMMENT ON COLUMN food_safety.inspection_violation_dim.inspection_date IS 'Semantic type: :date';

COMMENT ON COLUMN food_safety.inspection_violation_dim.phone IS 'Semantic type: :phone';

COMMENT ON COLUMN food_safety.inspection_violation_dim.follow_up_date IS 'Semantic type: :date';

-- Maintenance function for SCD Type 2 updates

CREATE OR REPLACE FUNCTION food_safety.update_dimension_scd()

RETURNS TRIGGER AS $$

BEGIN

    -- Close current record

    UPDATE 

food_safety

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