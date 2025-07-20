-- Create schema

CREATE SCHEMA IF NOT EXISTS organizational_data;



-- Set search path

SET search_path TO organizational_data, public;

CREATE TABLE organizational_data.name_phone_dim (
    id BIGSERIAL PRIMARY KEY,
    hire_date TEXT NOT NULL UNIQUE,
    salary TEXT NOT NULL UNIQUE,
    phone_mobile TEXT NOT NULL UNIQUE,
    employee_id TEXT NOT NULL UNIQUE,
    phone_work TEXT NOT NULL UNIQUE,
    department_id TEXT NOT NULL UNIQUE,
    work_address TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    commission_rate TEXT NOT NULL UNIQUE,
    emergency_contact_phone TEXT NOT NULL UNIQUE,
    department_name TEXT NOT NULL UNIQUE,
    ssn TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL UNIQUE,
    position_title TEXT NOT NULL UNIQUE,
    last_name TEXT NOT NULL UNIQUE,
    office_location TEXT NOT NULL UNIQUE,
    manager_id TEXT,
    emergency_contact_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE organizational_data.name_phone_dim IS 'Auto-generated table from semantic analysis';

COMMENT ON COLUMN organizational_data.name_phone_dim.hire_date IS 'Semantic type: :date';

COMMENT ON COLUMN organizational_data.name_phone_dim.salary IS 'Semantic type: :currency';

COMMENT ON COLUMN organizational_data.name_phone_dim.phone_mobile IS 'Semantic type: :phone';

COMMENT ON COLUMN organizational_data.name_phone_dim.phone_work IS 'Semantic type: :phone';

COMMENT ON COLUMN organizational_data.name_phone_dim.email IS 'Semantic type: :email';

COMMENT ON COLUMN organizational_data.name_phone_dim.commission_rate IS 'Semantic type: :currency';

COMMENT ON COLUMN organizational_data.name_phone_dim.emergency_contact_phone IS 'Semantic type: :phone';

COMMENT ON COLUMN organizational_data.name_phone_dim.ssn IS 'Semantic type: :ssn';

-- Maintenance function for SCD Type 2 updates

CREATE OR REPLACE FUNCTION organizational_data.update_dimension_scd()

RETURNS TRIGGER AS $$

BEGIN

    -- Close current record

    UPDATE 

organizational_data

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