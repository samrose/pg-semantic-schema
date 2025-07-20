-- Create schema

CREATE SCHEMA IF NOT EXISTS production_data;



-- Set search path

SET search_path TO production_data, public;

CREATE TABLE production_data.production_standard_dim (
    id BIGSERIAL PRIMARY KEY,
    corrective_action_required TEXT NOT NULL UNIQUE,
    inspector_name TEXT NOT NULL UNIQUE,
    inspection_id TEXT NOT NULL UNIQUE,
    certification_body TEXT NOT NULL UNIQUE,
    production_end_time TEXT NOT NULL UNIQUE,
    test_temperature_f TEXT NOT NULL UNIQUE,
    defect_count TEXT NOT NULL UNIQUE,
    scrap_quantity TEXT NOT NULL UNIQUE,
    inspection_date TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL UNIQUE,
    machine_id TEXT NOT NULL UNIQUE,
    production_supervisor TEXT NOT NULL UNIQUE,
    severity_level TEXT NOT NULL UNIQUE,
    dimensional_tolerance_mm TEXT NOT NULL UNIQUE,
    test_pressure_psi TEXT NOT NULL UNIQUE,
    inspector_id TEXT NOT NULL UNIQUE,
    operator_id TEXT NOT NULL UNIQUE,
    rework_possible TEXT NOT NULL UNIQUE,
    calibration_date TEXT NOT NULL UNIQUE,
    pass_fail_status TEXT NOT NULL UNIQUE,
    raw_material_lot TEXT NOT NULL UNIQUE,
    compliance_standard TEXT NOT NULL UNIQUE,
    defect_type TEXT NOT NULL UNIQUE,
    production_start_time TEXT NOT NULL UNIQUE,
    quality_standard TEXT NOT NULL UNIQUE,
    weight_tolerance_g TEXT NOT NULL UNIQUE,
    product_code TEXT NOT NULL UNIQUE,
    quality_manager TEXT NOT NULL UNIQUE,
    material_supplier TEXT NOT NULL UNIQUE,
    shift TEXT NOT NULL UNIQUE,
    supplier_batch_id TEXT NOT NULL UNIQUE,
    batch_number TEXT NOT NULL UNIQUE,
    production_line TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE production_data.production_standard_dim IS 'Auto-generated table from semantic analysis';

COMMENT ON COLUMN production_data.production_standard_dim.test_temperature_f IS 'Semantic type: :currency';

COMMENT ON COLUMN production_data.production_standard_dim.defect_count IS 'Semantic type: :currency';

COMMENT ON COLUMN production_data.production_standard_dim.scrap_quantity IS 'Semantic type: :currency';

COMMENT ON COLUMN production_data.production_standard_dim.inspection_date IS 'Semantic type: :date';

COMMENT ON COLUMN production_data.production_standard_dim.test_pressure_psi IS 'Semantic type: :currency';

COMMENT ON COLUMN production_data.production_standard_dim.calibration_date IS 'Semantic type: :date';

-- Maintenance function for SCD Type 2 updates

CREATE OR REPLACE FUNCTION production_data.update_dimension_scd()

RETURNS TRIGGER AS $$

BEGIN

    -- Close current record

    UPDATE 

production_data

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