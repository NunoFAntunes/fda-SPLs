-- FDA SPL Database Schema
-- This schema captures structured product label information with proper normalization

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Core medications table (enhanced)
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
    
    -- Enhanced document metadata
    document_code VARCHAR(50),
    document_code_system VARCHAR(100),
    document_display_name VARCHAR(200),
    processed_at TIMESTAMP WITH TIME ZONE,
    validation_passed BOOLEAN DEFAULT FALSE,
    validation_errors TEXT,
    processing_quality_score DECIMAL(3,2),
    
    -- File handling
    raw_xml_path VARCHAR(1000),
    processing_status VARCHAR(50) DEFAULT 'pending',
    processing_errors TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique combination of set_id and version
    UNIQUE(set_id, version)
);

-- Organizations (manufacturers, labelers, packers)
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_name VARCHAR(500) NOT NULL,
    duns_number VARCHAR(20),
    fda_establishment_id VARCHAR(50),
    organization_type VARCHAR(50) CHECK (organization_type IN ('manufacturer', 'labeler', 'packer', 'distributor')),
    address TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(organization_name, organization_type)
);

-- Junction table for medication-organization relationships
CREATE TABLE medication_organizations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,
    role VARCHAR(50) CHECK (role IN ('manufacturer', 'labeler', 'packer', 'distributor')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(spl_id, organization_id, role)
);

-- Substance reference table (UNII codes and names)
CREATE TABLE substances (
    unii_code VARCHAR(20) PRIMARY KEY,
    substance_name VARCHAR(500) NOT NULL,
    substance_type VARCHAR(50) CHECK (substance_type IN ('chemical', 'protein', 'mixture', 'polymer')),
    molecular_formula VARCHAR(200),
    cas_number VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ingredients table (active and inactive)
CREATE TABLE ingredients (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    ingredient_type VARCHAR(20) CHECK (ingredient_type IN ('active', 'inactive')) NOT NULL,
    substance_name VARCHAR(500) NOT NULL,
    unii_code VARCHAR(20) REFERENCES substances(unii_code),
    strength_numerator DECIMAL(15,6),
    strength_numerator_unit VARCHAR(20),
    strength_denominator DECIMAL(15,6) DEFAULT 1,
    strength_denominator_unit VARCHAR(20),
    active_moiety_unii VARCHAR(20) REFERENCES substances(unii_code),
    active_moiety_name VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique ingredient per medication
    UNIQUE(spl_id, substance_name, ingredient_type)
);

-- SPL sections (hierarchical LOINC-coded sections)
CREATE TABLE spl_sections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    section_id VARCHAR(255) NOT NULL, -- From SPL XML
    loinc_code VARCHAR(20), -- Section type identifier (e.g., 55106-9 for active ingredients)
    section_title VARCHAR(500),
    section_text TEXT,
    effective_time DATE,
    parent_section_id UUID REFERENCES spl_sections(id),
    section_order INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(spl_id, section_id)
);

-- Media references (images, diagrams in SPL documents)
CREATE TABLE media_references (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    section_id UUID REFERENCES spl_sections(id),
    media_id VARCHAR(100) NOT NULL,
    media_type VARCHAR(50),
    reference_value VARCHAR(500),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Regulatory approval information
CREATE TABLE regulatory_approvals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    approval_id VARCHAR(50) NOT NULL, -- ANDA numbers, NDA numbers, etc.
    approval_type VARCHAR(20) CHECK (approval_type IN ('ANDA', 'NDA', 'BLA', 'OTC')),
    approval_date DATE,
    territory_code VARCHAR(10) DEFAULT 'USA',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(spl_id, approval_id)
);

-- Marketing information
CREATE TABLE marketing_info (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    marketing_status VARCHAR(20) CHECK (marketing_status IN ('active', 'inactive', 'discontinued')),
    marketing_date_start DATE,
    marketing_date_end DATE,
    marketing_category VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Enhanced NDC codes and packaging information
CREATE TABLE ndc_codes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    spl_id VARCHAR(255) REFERENCES medications(spl_id) ON DELETE CASCADE,
    ndc_code VARCHAR(20) NOT NULL,
    package_description TEXT,
    strength VARCHAR(200),
    dosage_form VARCHAR(100),
    package_size VARCHAR(100),
    package_quantity DECIMAL(10,3),
    package_unit VARCHAR(50),
    container_type VARCHAR(100),
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
-- Medications table indexes
CREATE INDEX idx_medications_brand_name ON medications USING gin (brand_name gin_trgm_ops);
CREATE INDEX idx_medications_generic_name ON medications USING gin (generic_name gin_trgm_ops);
CREATE INDEX idx_medications_manufacturer ON medications USING gin (manufacturer_name gin_trgm_ops);
CREATE INDEX idx_medications_effective_date ON medications (effective_date);
CREATE INDEX idx_medications_processing_status ON medications (processing_status);
CREATE INDEX idx_medications_validation_passed ON medications (validation_passed);
CREATE INDEX idx_medications_set_id ON medications (set_id);

-- Organizations and relationships
CREATE INDEX idx_organizations_name ON organizations USING gin (organization_name gin_trgm_ops);
CREATE INDEX idx_organizations_type ON organizations (organization_type);
CREATE INDEX idx_medication_organizations_spl_id ON medication_organizations (spl_id);

-- Substances and ingredients
CREATE INDEX idx_substances_name ON substances USING gin (substance_name gin_trgm_ops);
CREATE INDEX idx_substances_unii ON substances (unii_code);
CREATE INDEX idx_ingredients_spl_id ON ingredients (spl_id);
CREATE INDEX idx_ingredients_type ON ingredients (ingredient_type);
CREATE INDEX idx_ingredients_unii ON ingredients (unii_code);
CREATE INDEX idx_ingredients_substance_name ON ingredients USING gin (substance_name gin_trgm_ops);

-- SPL sections
CREATE INDEX idx_spl_sections_spl_id ON spl_sections (spl_id);
CREATE INDEX idx_spl_sections_loinc_code ON spl_sections (loinc_code);
CREATE INDEX idx_spl_sections_parent ON spl_sections (parent_section_id);
CREATE INDEX idx_spl_sections_text ON spl_sections USING gin (section_text gin_trgm_ops);

-- NDC codes
CREATE INDEX idx_ndc_codes_ndc ON ndc_codes (ndc_code);
CREATE INDEX idx_ndc_codes_spl_id ON ndc_codes (spl_id);
CREATE INDEX idx_ndc_codes_dosage_form ON ndc_codes (dosage_form);

-- Regulatory and marketing
CREATE INDEX idx_regulatory_approvals_spl_id ON regulatory_approvals (spl_id);
CREATE INDEX idx_regulatory_approvals_type ON regulatory_approvals (approval_type);
CREATE INDEX idx_marketing_info_spl_id ON marketing_info (spl_id);
CREATE INDEX idx_marketing_info_status ON marketing_info (marketing_status);

-- Adverse effects
CREATE INDEX idx_adverse_effects_name ON adverse_effects USING gin (effect_name gin_trgm_ops);
CREATE INDEX idx_adverse_effects_system ON adverse_effects (system_organ_class);

-- Drug interactions
CREATE INDEX idx_drug_interactions_substance ON drug_interactions USING gin (interacting_substance gin_trgm_ops);
CREATE INDEX idx_drug_interactions_severity ON drug_interactions (severity);

-- Clinical data
CREATE INDEX idx_indications_condition ON indications USING gin (condition_name gin_trgm_ops);
CREATE INDEX idx_indications_spl_id ON indications (spl_id);
CREATE INDEX idx_warnings_spl_id ON warnings (spl_id);
CREATE INDEX idx_warnings_type ON warnings (warning_type);
CREATE INDEX idx_contraindications_spl_id ON contraindications (spl_id);
CREATE INDEX idx_dosages_spl_id ON dosages (spl_id);

-- LLM summaries
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

-- Enhanced medication search view
CREATE VIEW medication_search AS
SELECT 
    m.spl_id,
    m.set_id,
    m.version,
    m.brand_name,
    m.generic_name,
    m.manufacturer_name,
    m.effective_date,
    m.validation_passed,
    m.processing_quality_score,
    
    -- Aggregated data
    array_agg(DISTINCT n.ndc_code) FILTER (WHERE n.ndc_code IS NOT NULL) as ndc_codes,
    array_agg(DISTINCT i.condition_name) FILTER (WHERE i.condition_name IS NOT NULL) as conditions,
    array_agg(DISTINCT ing.substance_name) FILTER (WHERE ing.ingredient_type = 'active' AND ing.substance_name IS NOT NULL) as active_ingredients,
    array_agg(DISTINCT org.organization_name) FILTER (WHERE org.organization_name IS NOT NULL) as organizations,
    array_agg(DISTINCT ra.approval_type) FILTER (WHERE ra.approval_type IS NOT NULL) as approval_types,
    
    -- Counts
    COUNT(DISTINCT n.id) as ndc_count,
    COUNT(DISTINCT ing.id) FILTER (WHERE ing.ingredient_type = 'active') as active_ingredient_count,
    COUNT(DISTINCT w.id) as warning_count,
    
    -- Routes and forms
    m.route_of_administration,
    array_agg(DISTINCT n.dosage_form) FILTER (WHERE n.dosage_form IS NOT NULL) as dosage_forms
    
FROM medications m
LEFT JOIN ndc_codes n ON m.spl_id = n.spl_id
LEFT JOIN indications i ON m.spl_id = i.spl_id
LEFT JOIN ingredients ing ON m.spl_id = ing.spl_id
LEFT JOIN medication_organizations mo ON m.spl_id = mo.spl_id
LEFT JOIN organizations org ON mo.organization_id = org.id
LEFT JOIN regulatory_approvals ra ON m.spl_id = ra.spl_id
LEFT JOIN warnings w ON m.spl_id = w.spl_id
WHERE m.processing_status = 'completed'
GROUP BY m.spl_id, m.set_id, m.version, m.brand_name, m.generic_name, m.manufacturer_name, 
         m.effective_date, m.validation_passed, m.processing_quality_score, m.route_of_administration;

-- Additional specialized views
CREATE VIEW active_ingredients_summary AS
SELECT 
    s.unii_code,
    s.substance_name,
    COUNT(DISTINCT i.spl_id) as medication_count,
    array_agg(DISTINCT m.brand_name) FILTER (WHERE m.brand_name IS NOT NULL) as brand_names,
    array_agg(DISTINCT n.dosage_form) FILTER (WHERE n.dosage_form IS NOT NULL) as dosage_forms
FROM substances s
JOIN ingredients i ON s.unii_code = i.unii_code
JOIN medications m ON i.spl_id = m.spl_id
LEFT JOIN ndc_codes n ON m.spl_id = n.spl_id
WHERE i.ingredient_type = 'active' 
  AND m.processing_status = 'completed'
GROUP BY s.unii_code, s.substance_name
ORDER BY medication_count DESC;

-- Section content view for text search
CREATE VIEW section_search AS
SELECT 
    s.spl_id,
    m.brand_name,
    m.generic_name,
    s.loinc_code,
    s.section_title,
    s.section_text,
    CASE s.loinc_code
        WHEN '55106-9' THEN 'Active Ingredients'
        WHEN '34071-1' THEN 'Warnings'
        WHEN '34067-9' THEN 'Indications and Usage'
        WHEN '50570-1' THEN 'Do Not Use'
        WHEN '50569-3' THEN 'Ask Doctor'
        ELSE s.section_title
    END as section_type_name
FROM spl_sections s
JOIN medications m ON s.spl_id = m.spl_id
WHERE m.processing_status = 'completed'
  AND s.section_text IS NOT NULL;