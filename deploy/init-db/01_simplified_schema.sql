-- FDA SPL Simplified Database Schema
-- Optimized for Phase 4 database integration with LLM retrieval focus
-- Simple structure for essential SPL data only

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Core medications table
CREATE TABLE medications (
    spl_id VARCHAR(255) PRIMARY KEY,
    set_id VARCHAR(255) NOT NULL,
    version_number INTEGER NOT NULL DEFAULT 1,
    effective_date DATE,
    
    -- Product information
    brand_name VARCHAR(500),
    generic_name VARCHAR(500),
    manufacturer VARCHAR(500),
    labeler VARCHAR(500),
    
    -- Primary product identifiers
    ndc_code VARCHAR(20), -- Primary NDC code
    product_form VARCHAR(100), -- TABLET, CAPSULE, etc.
    route_of_administration VARCHAR(100), -- ORAL, TOPICAL, etc.
    
    -- Regulatory information
    approval_type VARCHAR(50), -- OTC, NDA, ANDA, etc.
    approval_id VARCHAR(50), -- Approval number
    marketing_status VARCHAR(20) CHECK (marketing_status IN ('active', 'inactive', 'discontinued')),
    marketing_date_start DATE,
    
    -- Document metadata
    document_code VARCHAR(50),
    document_display_name VARCHAR(200),
    
    -- Processing information
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processing_status VARCHAR(50) DEFAULT 'completed',
    processing_errors TEXT,
    
    -- Flexible storage for additional data
    additional_data JSONB,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique combination of set_id and version
    UNIQUE(set_id, version_number)
);

-- Ingredients table (active and inactive)
CREATE TABLE ingredients (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    
    -- Ingredient classification
    ingredient_type VARCHAR(20) CHECK (ingredient_type IN ('active', 'inactive')) NOT NULL,
    
    -- Substance information
    substance_name VARCHAR(500) NOT NULL,
    unii_code VARCHAR(20), -- FDA Unique Ingredient Identifier
    
    -- Strength/quantity information
    strength_numerator DECIMAL(15,6),
    strength_numerator_unit VARCHAR(50),
    strength_denominator DECIMAL(15,6) DEFAULT 1,
    strength_denominator_unit VARCHAR(50),
    
    -- Active moiety information (for active ingredients)
    active_moiety_name VARCHAR(500),
    active_moiety_unii VARCHAR(20),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique ingredient per medication
    UNIQUE(spl_id, substance_name, ingredient_type)
);

-- SPL sections for text content and LLM retrieval
CREATE TABLE spl_sections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    
    -- Section identification
    section_id VARCHAR(255), -- Original XML section ID
    loinc_code VARCHAR(20), -- LOINC section type code
    section_title VARCHAR(500),
    section_text TEXT,
    
    -- Section metadata
    section_order INTEGER, -- Order within document
    effective_time DATE,
    
    -- Hierarchy support (for subsections)
    parent_section_id UUID REFERENCES spl_sections(id),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(spl_id, section_id)
);

-- Indications table for structured indication storage
CREATE TABLE indications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    
    -- Indication content
    indication_text TEXT NOT NULL,
    indication_type VARCHAR(50) CHECK (indication_type IN ('primary', 'secondary', 'prevention')),
    
    -- Optional structured fields (can be populated later)
    condition_name VARCHAR(300),
    population_restriction VARCHAR(200),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Additional NDC codes (for medications with multiple NDCs)
CREATE TABLE additional_ndc_codes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    
    ndc_code VARCHAR(20) NOT NULL,
    package_description TEXT,
    package_size VARCHAR(100),
    package_quantity DECIMAL(10,3),
    package_unit VARCHAR(50),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(spl_id, ndc_code)
);

-- Create indexes for performance
-- Medications table indexes
CREATE INDEX idx_medications_brand_name ON medications USING gin (brand_name gin_trgm_ops);
CREATE INDEX idx_medications_generic_name ON medications USING gin (generic_name gin_trgm_ops);
CREATE INDEX idx_medications_manufacturer ON medications USING gin (manufacturer gin_trgm_ops);
CREATE INDEX idx_medications_ndc_code ON medications (ndc_code);
CREATE INDEX idx_medications_set_id ON medications (set_id);
CREATE INDEX idx_medications_effective_date ON medications (effective_date);
CREATE INDEX idx_medications_processing_status ON medications (processing_status);

-- Ingredients indexes
CREATE INDEX idx_ingredients_spl_id ON ingredients (spl_id);
CREATE INDEX idx_ingredients_type ON ingredients (ingredient_type);
CREATE INDEX idx_ingredients_substance_name ON ingredients USING gin (substance_name gin_trgm_ops);
CREATE INDEX idx_ingredients_unii ON ingredients (unii_code);

-- Sections indexes
CREATE INDEX idx_spl_sections_spl_id ON spl_sections (spl_id);
CREATE INDEX idx_spl_sections_loinc_code ON spl_sections (loinc_code);
CREATE INDEX idx_spl_sections_text ON spl_sections USING gin (section_text gin_trgm_ops);
CREATE INDEX idx_spl_sections_parent ON spl_sections (parent_section_id);

-- Indications indexes
CREATE INDEX idx_indications_spl_id ON indications (spl_id);
CREATE INDEX idx_indications_text ON indications USING gin (indication_text gin_trgm_ops);
CREATE INDEX idx_indications_condition ON indications USING gin (condition_name gin_trgm_ops);

-- Additional NDC codes indexes
CREATE INDEX idx_additional_ndc_codes_spl_id ON additional_ndc_codes (spl_id);
CREATE INDEX idx_additional_ndc_codes_ndc ON additional_ndc_codes (ndc_code);

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

-- Simplified medication search view for LLM queries
CREATE VIEW medication_search AS
SELECT 
    m.spl_id,
    m.set_id,
    m.version_number,
    m.brand_name,
    m.generic_name,
    m.manufacturer,
    m.ndc_code,
    m.product_form,
    m.route_of_administration,
    m.approval_type,
    m.marketing_status,
    m.effective_date,
    
    -- Aggregated ingredients
    array_agg(DISTINCT i.substance_name) FILTER (WHERE i.ingredient_type = 'active') as active_ingredients,
    array_agg(DISTINCT i.substance_name) FILTER (WHERE i.ingredient_type = 'inactive') as inactive_ingredients,
    
    -- Aggregated indications
    array_agg(DISTINCT ind.indication_text) FILTER (WHERE ind.indication_text IS NOT NULL) as indications,
    
    -- Additional NDC codes
    array_agg(DISTINCT andc.ndc_code) FILTER (WHERE andc.ndc_code IS NOT NULL) as additional_ndcs
    
FROM medications m
LEFT JOIN ingredients i ON m.spl_id = i.spl_id
LEFT JOIN indications ind ON m.spl_id = ind.spl_id
LEFT JOIN additional_ndc_codes andc ON m.spl_id = andc.spl_id
WHERE m.processing_status = 'completed'
GROUP BY m.spl_id, m.set_id, m.version_number, m.brand_name, m.generic_name, 
         m.manufacturer, m.ndc_code, m.product_form, m.route_of_administration,
         m.approval_type, m.marketing_status, m.effective_date;

-- Section content view for LLM text retrieval
CREATE VIEW section_content AS
SELECT 
    s.spl_id,
    m.brand_name,
    m.generic_name,
    s.loinc_code,
    s.section_title,
    s.section_text,
    s.section_order,
    CASE s.loinc_code
        WHEN '55106-9' THEN 'Active Ingredients'
        WHEN '34071-1' THEN 'Warnings'
        WHEN '34067-9' THEN 'Indications and Usage'
        WHEN '34068-7' THEN 'Dosage and Administration'
        WHEN '50570-1' THEN 'Do Not Use'
        WHEN '50569-3' THEN 'Ask Doctor Before Use'
        WHEN '50567-7' THEN 'When Using This Product'
        WHEN '50565-1' THEN 'Keep Out of Reach of Children'
        WHEN '44425-7' THEN 'Storage and Handling'
        WHEN '51727-6' THEN 'Inactive Ingredients'
        WHEN '51945-4' THEN 'Package Label'
        ELSE COALESCE(s.section_title, 'Other Section')
    END as section_type_name
FROM spl_sections s
JOIN medications m ON s.spl_id = m.spl_id
WHERE m.processing_status = 'completed'
  AND s.section_text IS NOT NULL
ORDER BY s.spl_id, s.section_order;

-- Active ingredients summary view
CREATE VIEW active_ingredients_summary AS
SELECT 
    i.substance_name,
    i.unii_code,
    COUNT(DISTINCT i.spl_id) as medication_count,
    array_agg(DISTINCT m.brand_name) FILTER (WHERE m.brand_name IS NOT NULL) as brand_names,
    array_agg(DISTINCT m.product_form) FILTER (WHERE m.product_form IS NOT NULL) as dosage_forms,
    array_agg(DISTINCT m.manufacturer) FILTER (WHERE m.manufacturer IS NOT NULL) as manufacturers
FROM ingredients i
JOIN medications m ON i.spl_id = m.spl_id
WHERE i.ingredient_type = 'active' 
  AND m.processing_status = 'completed'
GROUP BY i.substance_name, i.unii_code
HAVING COUNT(DISTINCT i.spl_id) > 0
ORDER BY medication_count DESC, i.substance_name;