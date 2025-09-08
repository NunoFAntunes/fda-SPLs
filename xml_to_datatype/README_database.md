# XML to Database Converter

This script converts FDA SPL XML documents to typed Python dataclass objects and saves the structured data to a PostgreSQL database.

## Prerequisites

1. **Database Setup**: Start the PostgreSQL container using Docker Compose:
   ```bash
   cd deploy
   docker-compose up -d postgres
   ```

2. **Python Dependencies**: Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **Database Initialization**: The database schema will be automatically created when the container starts (from `deploy/init-db/create_database.sql`).

## Usage

### Basic Usage
```bash
python xml_to_database.py path/to/spl_document.xml
```

### With Custom Database Connection
```bash
python xml_to_database.py path/to/spl_document.xml \
  --host localhost \
  --port 5432 \
  --database fda_spls \
  --user postgres \
  --password postgres
```

### With Verbose Logging
```bash
python xml_to_database.py path/to/spl_document.xml --verbose
```

## What Gets Stored

The script extracts and stores only the **structured data** from SPL documents:

### Document Level
- Document ID, codes, title, effective date
- Author organization information
- Source file path

### Product Information
- Product codes (NDC), names, dosage forms
- Administration routes
- Generic medicine names

### Ingredients
- Active and inactive ingredients with quantities
- UNII substance codes and names
- Active moieties

### Regulatory Data
- FDA approvals and application numbers
- Marketing authorization status
- Territorial authorities

### Physical Characteristics
- Color, shape, size, imprint text
- Scored tablet information
- Flavor codes

### Packaging
- Package quantities and container types
- NDC package codes
- Container form codes (bottle, box, etc.)

### Media References
- Product images and their descriptions
- Media file references

## What Is NOT Stored

- **Free text sections** (warnings, indications, directions, etc.)
- **HTML content** from clinical sections
- **Conversion metadata** and processing notes

This design focuses on structured, queryable pharmaceutical data while excluding variable text content.

## Database Schema

The script uses the database schema defined in `deploy/init-db/create_database.sql`. Key tables include:

- `documents` - Root SPL document information
- `organizations` - Manufacturing companies
- `products` - Manufactured products
- `ingredients` - Product ingredients with quantities
- `ingredient_substances` - Normalized substance definitions
- `active_moieties` - Active pharmaceutical ingredients
- `product_characteristics` - Physical properties
- `packaging_configurations` - Package types and sizes
- `approvals` - Regulatory approvals
- `marketing_acts` - Marketing authorization status
- `observation_media` - Product images and media

## Error Handling

- **Transaction Management**: All database operations are wrapped in transactions
- **Rollback on Error**: Failed imports are automatically rolled back
- **Data Validation**: Invalid dates and numeric values are handled gracefully
- **Connection Management**: Database connections are properly closed
- **Logging**: Comprehensive logging with configurable verbosity

## Example Output

```bash
$ python xml_to_database.py example.xml --verbose

2024-01-15 10:30:15 - INFO - Converting XML file: example.xml
2024-01-15 10:30:16 - INFO - Converted document with 1 products
2024-01-15 10:30:16 - INFO - Connected to database successfully
2024-01-15 10:30:16 - INFO - Starting import of document: f119f7a7-c1a8-44a6-a678-0e13c46294fd
2024-01-15 10:30:17 - INFO - Successfully imported document: f119f7a7-c1a8-44a6-a678-0e13c46294fd
2024-01-15 10:30:17 - INFO - Disconnected from database
SUCCESS: Document imported to database
```

## Querying the Data

Once imported, you can query the structured data using SQL:

```sql
-- Find all active ingredients for a product
SELECT p.name_text, s.substance_name, i.quantity_numerator_value, i.quantity_numerator_unit
FROM products p
JOIN ingredients i ON p.id = i.product_id
JOIN ingredient_substances s ON i.ingredient_substance_id = s.id
WHERE i.class_code = 'ACTIM' AND p.name_text ILIKE '%bonine%';

-- Find products by color
SELECT p.name_text, pc.value_display_name as color
FROM products p
JOIN product_characteristics pc ON p.id = pc.product_id
WHERE pc.characteristic_code = 'SPLCOLOR';

-- Find products by manufacturer
SELECT p.name_text, o.name as manufacturer
FROM products p
JOIN documents d ON p.document_id = d.id
JOIN organizations o ON d.author_organization_id = o.id
WHERE o.name ILIKE '%insight%';
```