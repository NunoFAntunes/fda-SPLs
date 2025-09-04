# FDA SPL Database Deployment

This directory contains the Docker Compose configuration and database initialization scripts for the FDA Structured Product Labels project.

## Quick Start

Start the PostgreSQL database:

```bash
cd deploy
docker-compose up -d
```

The database will be available at `localhost:5432` with:
- **Database**: `fda_spls`
- **Username**: `postgres`
- **Password**: `postgres`

## Database Schema Overview

The database is designed with proper normalization to efficiently store and query FDA SPL data:

### Core Tables

- **`medications`** - Main medication information (SPL ID, names, manufacturer, etc.)
- **`ndc_codes`** - NDC codes and packaging information linked to medications
- **`raw_sections`** - Original XML sections for reference and debugging

### Normalized Reference Data

- **`adverse_effects`** - Normalized adverse effects that can be shared across medications
- **`drug_interactions`** - Normalized drug interaction patterns

### Junction Tables

- **`medication_adverse_effects`** - Links medications to adverse effects with context
- **`medication_drug_interactions`** - Links medications to drug interactions

### Structured Information Tables

- **`indications`** - What conditions the medication treats
- **`dosages`** - Dosage and administration information by population
- **`contraindications`** - When not to use the medication
- **`warnings`** - Warnings and precautions (including black box warnings)
- **`storage_conditions`** - Storage and handling requirements
- **`population_specific_use`** - Special populations (pregnancy, pediatric, etc.)

### AI/LLM Integration

- **`llm_summaries`** - Generated patient-friendly summaries at different reading levels

## Key Design Decisions

### Normalization Benefits
- **Adverse Effects**: Common side effects (e.g., "headache", "nausea") are stored once and referenced by multiple medications
- **Drug Interactions**: Interaction patterns are normalized and can be reused across medications
- **Efficient Storage**: Reduces data redundancy while maintaining query performance

### Search Optimization
- Full-text search indexes on medication names using PostgreSQL's trigram similarity
- Dedicated search view (`medication_search`) for common query patterns
- Proper indexing on foreign keys and frequently queried columns

### Data Integrity
- Foreign key constraints ensure referential integrity
- Check constraints on enum-like fields (severity levels, warning types, etc.)
- Unique constraints prevent duplicate associations

### Extensibility
- JSONB column in `llm_summaries` for flexible AI-generated content
- Array fields for multi-value attributes (routes of administration)
- Separate tables for each major SPL section allow independent processing

## Database Connection Examples

### Python (psycopg2)
```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="fda_spls",
    user="postgres",
    password="postgres"
)
```

### Python (SQLAlchemy)
```python
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@localhost:5432/fda_spls")
```

## Common Queries

### Search medications by brand name
```sql
SELECT * FROM medication_search 
WHERE brand_name ILIKE '%aspirin%';
```

### Find medications with specific adverse effects
```sql
SELECT DISTINCT m.brand_name, m.generic_name, ae.effect_name
FROM medications m
JOIN medication_adverse_effects mae ON m.spl_id = mae.spl_id
JOIN adverse_effects ae ON mae.adverse_effect_id = ae.id
WHERE ae.effect_name ILIKE '%headache%';
```

### Get complete medication profile
```sql
SELECT 
    m.*,
    array_agg(DISTINCT n.ndc_code) as ndc_codes,
    array_agg(DISTINCT i.indication_text) as indications
FROM medications m
LEFT JOIN ndc_codes n ON m.spl_id = n.spl_id
LEFT JOIN indications i ON m.spl_id = i.spl_id
WHERE m.spl_id = 'your-spl-id'
GROUP BY m.spl_id;
```

## Maintenance

### Backup Database
```bash
docker exec fda-spls-postgres pg_dump -U postgres fda_spls > backup.sql
```

### Restore Database
```bash
docker exec -i fda-spls-postgres psql -U postgres fda_spls < backup.sql
```

### Access Database Shell
```bash
docker exec -it fda-spls-postgres psql -U postgres -d fda_spls
```

## Monitoring

Check database health:
```bash
docker-compose ps
```

View logs:
```bash
docker-compose logs postgres
```

## Future Enhancements

The schema is designed to support:
- Version tracking of SPL updates
- FHIR MedicationKnowledge export
- Advanced search with Elasticsearch integration
- ML-based drug interaction detection
- Regulatory submission tracking