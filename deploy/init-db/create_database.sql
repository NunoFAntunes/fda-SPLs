-- FDA SPL Structured Data Database Schema
-- PostgreSQL implementation for storing structured pharmaceutical product data

-- Drop tables in reverse dependency order if they exist
DROP TABLE IF EXISTS observation_media CASCADE;
DROP TABLE IF EXISTS product_characteristics CASCADE;
DROP TABLE IF EXISTS marketing_acts CASCADE;
DROP TABLE IF EXISTS approvals CASCADE;
DROP TABLE IF EXISTS packaging_configurations CASCADE;
DROP TABLE IF EXISTS active_moieties CASCADE;
DROP TABLE IF EXISTS ingredients CASCADE;
DROP TABLE IF EXISTS ingredient_substances CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS organizations CASCADE;
DROP TABLE IF EXISTS documents CASCADE;
DROP TABLE IF EXISTS code_systems CASCADE;

-- Reference table for code systems
CREATE TABLE code_systems (
    id SERIAL PRIMARY KEY,
    code_system VARCHAR(100) UNIQUE NOT NULL,
    description TEXT
);

-- Organizations table
CREATE TABLE organizations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    id_root VARCHAR(100),
    id_extension VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(id_root, id_extension)
);

-- Documents table (root level)
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    document_id_root VARCHAR(100) UNIQUE NOT NULL,
    code VARCHAR(50),
    code_system VARCHAR(100),
    code_display_name VARCHAR(255),
    title_text TEXT,
    title_html TEXT,
    effective_time DATE,
    set_id_root VARCHAR(100),
    version_number INTEGER,
    author_organization_id INTEGER REFERENCES organizations(id),
    source_file_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    product_code VARCHAR(50),
    product_code_system VARCHAR(100),
    name_text VARCHAR(255),
    name_html TEXT,
    name_suffix VARCHAR(100),
    form_code VARCHAR(50),
    form_code_system VARCHAR(100),
    form_display_name VARCHAR(255),
    route_code VARCHAR(50),
    route_code_system VARCHAR(100),
    route_display_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generic medicines (normalized)
CREATE TABLE generic_medicines (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    generic_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ingredient substances (normalized for reuse)
CREATE TABLE ingredient_substances (
    id SERIAL PRIMARY KEY,
    substance_code VARCHAR(50),
    substance_code_system VARCHAR(100),
    substance_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(substance_code, substance_code_system)
);

-- Active moieties
CREATE TABLE active_moieties (
    id SERIAL PRIMARY KEY,
    ingredient_substance_id INTEGER NOT NULL REFERENCES ingredient_substances(id) ON DELETE CASCADE,
    moiety_code VARCHAR(50),
    moiety_code_system VARCHAR(100),
    moiety_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product ingredients
CREATE TABLE ingredients (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    ingredient_substance_id INTEGER NOT NULL REFERENCES ingredient_substances(id),
    class_code VARCHAR(10) NOT NULL, -- ACTIM (active) or IACT (inactive)
    quantity_numerator_value DECIMAL,
    quantity_numerator_unit VARCHAR(20),
    quantity_denominator_value DECIMAL,
    quantity_denominator_unit VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Packaging configurations
CREATE TABLE packaging_configurations (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    quantity_numerator_value INTEGER,
    quantity_numerator_unit VARCHAR(20),
    quantity_denominator_value INTEGER,
    translation_code VARCHAR(50),
    translation_code_system VARCHAR(100),
    translation_display_name VARCHAR(255),
    container_code VARCHAR(50),
    container_code_system VARCHAR(100),
    container_form_code VARCHAR(50),
    container_form_code_system VARCHAR(100),
    container_form_display_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product approvals
CREATE TABLE approvals (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    approval_id_root VARCHAR(100),
    approval_id_extension VARCHAR(100),
    approval_code VARCHAR(50),
    approval_code_system VARCHAR(100),
    approval_display_name VARCHAR(255),
    territory_code VARCHAR(10),
    territory_code_system VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Marketing acts
CREATE TABLE marketing_acts (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    marketing_code VARCHAR(50),
    marketing_code_system VARCHAR(100),
    status_code VARCHAR(20),
    effective_time_low DATE,
    effective_time_high DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product characteristics (color, shape, size, etc.)
CREATE TABLE product_characteristics (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    characteristic_code VARCHAR(50) NOT NULL, -- SPLCOLOR, SPLSHAPE, SPLSIZE, etc.
    characteristic_code_system VARCHAR(100),
    value_type VARCHAR(10), -- CE, INT, PQ, ST
    value_code VARCHAR(50),
    value_code_system VARCHAR(100),
    value_display_name VARCHAR(255),
    value_text TEXT,
    value_numeric DECIMAL,
    value_unit VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Observation media (images, etc.)
CREATE TABLE observation_media (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    media_id VARCHAR(50) NOT NULL,
    media_text VARCHAR(255),
    media_type VARCHAR(50),
    media_reference VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert common code systems for reference
INSERT INTO code_systems (code_system, description) VALUES
('2.16.840.1.113883.6.1', 'LOINC - Logical Observation Identifiers Names and Codes'),
('2.16.840.1.113883.6.69', 'NDC - National Drug Code'),
('2.16.840.1.113883.3.26.1.1', 'NCI Thesaurus'),
('2.16.840.1.113883.4.9', 'FDA UNII - Unique Ingredient Identifier'),
('2.16.840.1.113883.1.11.19255', 'SPL Product Data Elements'),
('2.16.840.1.113883.3.149', 'FDA Application Number'),
('2.16.840.1.113883.5.28', 'Country Code');

-- Create indexes for better query performance
CREATE INDEX idx_documents_document_id ON documents(document_id_root);
CREATE INDEX idx_products_document_id ON products(document_id);
CREATE INDEX idx_products_code ON products(product_code);
CREATE INDEX idx_ingredients_product_id ON ingredients(product_id);
CREATE INDEX idx_ingredients_substance_id ON ingredients(ingredient_substance_id);
CREATE INDEX idx_ingredients_class_code ON ingredients(class_code);
CREATE INDEX idx_ingredient_substances_code ON ingredient_substances(substance_code);
CREATE INDEX idx_ingredient_substances_name ON ingredient_substances(substance_name);
CREATE INDEX idx_packaging_product_id ON packaging_configurations(product_id);
CREATE INDEX idx_characteristics_product_id ON product_characteristics(product_id);
CREATE INDEX idx_characteristics_code ON product_characteristics(characteristic_code);
CREATE INDEX idx_organizations_name ON organizations(name);
CREATE INDEX idx_approvals_product_id ON approvals(product_id);
CREATE INDEX idx_marketing_acts_product_id ON marketing_acts(product_id);
CREATE INDEX idx_observation_media_document_id ON observation_media(document_id);

-- Create views for common queries
CREATE VIEW v_product_summary AS
SELECT 
    p.id as product_id,
    p.product_code,
    p.name_text as product_name,
    p.form_display_name,
    o.name as manufacturer,
    d.effective_time,
    COUNT(DISTINCT CASE WHEN i.class_code = 'ACTIM' THEN i.id END) as active_ingredients_count,
    COUNT(DISTINCT CASE WHEN i.class_code = 'IACT' THEN i.id END) as inactive_ingredients_count
FROM products p
JOIN documents d ON p.document_id = d.id
LEFT JOIN organizations o ON d.author_organization_id = o.id
LEFT JOIN ingredients i ON p.id = i.product_id
GROUP BY p.id, p.product_code, p.name_text, p.form_display_name, o.name, d.effective_time;

CREATE VIEW v_active_ingredients AS
SELECT 
    p.id as product_id,
    p.name_text as product_name,
    i.class_code,
    s.substance_name,
    i.quantity_numerator_value,
    i.quantity_numerator_unit,
    i.quantity_denominator_value,
    i.quantity_denominator_unit
FROM products p
JOIN ingredients i ON p.id = i.product_id
JOIN ingredient_substances s ON i.ingredient_substance_id = s.id
WHERE i.class_code = 'ACTIM';

COMMENT ON TABLE documents IS 'Root document information from SPL files';
COMMENT ON TABLE products IS 'Manufactured products with codes and names';
COMMENT ON TABLE ingredients IS 'Product ingredients (both active and inactive)';
COMMENT ON TABLE ingredient_substances IS 'Normalized substance names and codes';
COMMENT ON TABLE product_characteristics IS 'Physical and other structured characteristics';
COMMENT ON TABLE packaging_configurations IS 'Package sizes and container information';
COMMENT ON TABLE organizations IS 'Manufacturing organizations';
COMMENT ON TABLE approvals IS 'Regulatory approvals';
COMMENT ON TABLE marketing_acts IS 'Marketing status information';
COMMENT ON TABLE observation_media IS 'Associated media files and references';