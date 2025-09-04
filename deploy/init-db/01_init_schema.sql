-- FDA SPL Database Schema
-- This schema captures structured product label information with proper normalization

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Core medications table
CREATE TABLE medications (
    spl_id VARCHAR(255) PRIMARY KEY,
    set_id VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    effective_date DATE,
    brand_name VARCHAR(500),
    generic_name VARCHAR(500),
    manufacturer_name VARCHAR(500),
    labeler_name VARCHAR(500),
    product_type VARCHAR(100),
    route_of_administration TEXT[],
    raw_xml_path VARCHAR(1000),
    processing_status VARCHAR(50) DEFAULT 'pending',
    processing_errors TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique combination of set_id and version
    UNIQUE(set_id, version)
);

-- NDC codes and packaging information
CREATE TABLE ndc_codes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    ndc_code VARCHAR(20) NOT NULL,
    package_description TEXT,
    strength VARCHAR(200),
    dosage_form VARCHAR(100),
    package_size VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique NDC per medication
    UNIQUE(spl_id, ndc_code)
);

-- Normalized adverse effects (reusable across medications)
CREATE TABLE adverse_effects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    effect_name VARCHAR(500) NOT NULL UNIQUE,
    system_organ_class VARCHAR(200),
    severity_category VARCHAR(50) CHECK (severity_category IN ('common', 'serious', 'rare', 'very_rare')),
    meddra_code VARCHAR(20),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Junction table linking medications to adverse effects
CREATE TABLE medication_adverse_effects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    adverse_effect_id UUID REFERENCES adverse_effects(id) ON DELETE CASCADE,
    frequency VARCHAR(100),
    severity VARCHAR(50),
    context_text TEXT,
    section_type VARCHAR(50) CHECK (section_type IN ('adverse_reactions', 'postmarketing', 'clinical_trials')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Prevent duplicate associations
    UNIQUE(spl_id, adverse_effect_id, section_type)
);

-- Normalized drug interactions
CREATE TABLE drug_interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interacting_substance VARCHAR(500) NOT NULL,
    interaction_type VARCHAR(100),
    severity VARCHAR(50) CHECK (severity IN ('major', 'moderate', 'minor', 'contraindicated')),
    mechanism TEXT,
    clinical_effect TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique interactions
    UNIQUE(interacting_substance, interaction_type, mechanism)
);

-- Junction table for medication drug interactions
CREATE TABLE medication_drug_interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    drug_interaction_id UUID REFERENCES drug_interactions(id) ON DELETE CASCADE,
    interaction_text TEXT,
    clinical_management TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(spl_id, drug_interaction_id)
);

-- Indications and usage
CREATE TABLE indications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    indication_text TEXT NOT NULL,
    indication_type VARCHAR(50) CHECK (indication_type IN ('primary', 'secondary', 'off_label')),
    condition_name VARCHAR(300),
    icd10_code VARCHAR(10),
    population_restriction TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Dosage and administration
CREATE TABLE dosages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    population VARCHAR(100) CHECK (population IN ('adult', 'pediatric', 'geriatric', 'general')),
    route VARCHAR(100),
    dosage_text TEXT NOT NULL,
    strength VARCHAR(200),
    frequency VARCHAR(200),
    duration VARCHAR(200),
    special_instructions TEXT,
    indication_specific TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Contraindications
CREATE TABLE contraindications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    contraindication_text TEXT NOT NULL,
    severity VARCHAR(50) CHECK (severity IN ('absolute', 'relative')) DEFAULT 'absolute',
    condition_name VARCHAR(300),
    population VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Warnings and precautions
CREATE TABLE warnings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    warning_text TEXT NOT NULL,
    warning_type VARCHAR(50) CHECK (warning_type IN ('black_box', 'warning', 'precaution')) NOT NULL,
    population_specific VARCHAR(100),
    severity VARCHAR(50),
    clinical_context TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Storage and handling
CREATE TABLE storage_conditions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    temperature_range VARCHAR(100),
    humidity_requirements VARCHAR(100),
    light_protection BOOLEAN,
    storage_text TEXT NOT NULL,
    special_handling TEXT,
    expiration_info TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Population-specific use (pregnancy, nursing, pediatric, geriatric)
CREATE TABLE population_specific_use (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    population VARCHAR(100) CHECK (population IN ('pregnancy', 'nursing', 'pediatric', 'geriatric', 'renal_impairment', 'hepatic_impairment')) NOT NULL,
    recommendation_text TEXT NOT NULL,
    safety_category VARCHAR(10),
    lactation_risk VARCHAR(50),
    age_range VARCHAR(100),
    special_considerations TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- LLM-generated summaries
CREATE TABLE llm_summaries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    reading_level VARCHAR(20) CHECK (reading_level IN ('grade6', 'grade9', 'professional')) NOT NULL,
    summary_data JSONB NOT NULL,
    model_used VARCHAR(100),
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    quality_score DECIMAL(3,2),
    factuality_verified BOOLEAN DEFAULT FALSE,
    
    -- Ensure one summary per medication per reading level
    UNIQUE(spl_id, reading_level)
);

-- Raw XML sections for reference
CREATE TABLE raw_sections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    section_name VARCHAR(200) NOT NULL,
    section_content TEXT NOT NULL,
    section_xpath VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(spl_id, section_name)
);

-- Create indexes for performance
CREATE INDEX idx_medications_brand_name ON medications USING gin (brand_name gin_trgm_ops);
CREATE INDEX idx_medications_generic_name ON medications USING gin (generic_name gin_trgm_ops);
CREATE INDEX idx_medications_manufacturer ON medications USING gin (manufacturer_name gin_trgm_ops);
CREATE INDEX idx_medications_effective_date ON medications (effective_date);
CREATE INDEX idx_medications_processing_status ON medications (processing_status);

CREATE INDEX idx_ndc_codes_ndc ON ndc_codes (ndc_code);
CREATE INDEX idx_ndc_codes_spl_id ON ndc_codes (spl_id);

CREATE INDEX idx_adverse_effects_name ON adverse_effects USING gin (effect_name gin_trgm_ops);
CREATE INDEX idx_adverse_effects_system ON adverse_effects (system_organ_class);

CREATE INDEX idx_drug_interactions_substance ON drug_interactions USING gin (interacting_substance gin_trgm_ops);
CREATE INDEX idx_drug_interactions_severity ON drug_interactions (severity);

CREATE INDEX idx_indications_condition ON indications USING gin (condition_name gin_trgm_ops);
CREATE INDEX idx_indications_spl_id ON indications (spl_id);

CREATE INDEX idx_llm_summaries_spl_reading ON llm_summaries (spl_id, reading_level);

-- Add updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add trigger to medications table
CREATE TRIGGER update_medications_updated_at BEFORE UPDATE ON medications
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create a view for easy medication search
CREATE VIEW medication_search AS
SELECT 
    m.spl_id,
    m.brand_name,
    m.generic_name,
    m.manufacturer_name,
    m.effective_date,
    array_agg(DISTINCT n.ndc_code) as ndc_codes,
    array_agg(DISTINCT i.condition_name) FILTER (WHERE i.condition_name IS NOT NULL) as conditions,
    m.route_of_administration
FROM medications m
LEFT JOIN ndc_codes n ON m.spl_id = n.spl_id
LEFT JOIN indications i ON m.spl_id = i.spl_id
WHERE m.processing_status = 'completed'
GROUP BY m.spl_id, m.brand_name, m.generic_name, m.manufacturer_name, m.effective_date, m.route_of_administration;