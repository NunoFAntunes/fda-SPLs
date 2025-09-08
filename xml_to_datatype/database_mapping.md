# SPL Dataclass to Database Mapping

This document describes how the structured fields from the Python dataclass objects map to the PostgreSQL database tables. Open text fields (like `free_text_sections`) are intentionally excluded from database storage.

## Table Mapping Overview

### 1. Document Level (`SPLDocument` → `documents`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `document.id.root` | `document_id_root` | Primary document identifier |
| `document.code.code` | `code` | Document type code |
| `document.code.codeSystem` | `code_system` | Code system OID |
| `document.code.displayName` | `code_display_name` | Human readable document type |
| `document.title.text` | `title_text` | Plain text title |
| `document.title.html` | `title_html` | HTML formatted title |
| `document.effectiveTime.value` | `effective_time` | Document effective date |
| `document.setId.root` | `set_id_root` | Document set identifier |
| `document.versionNumber.value` | `version_number` | Document version |
| `source_file.path` | `source_file_path` | Original XML file path |

**Example:**
```json
"document": {
  "id": {"root": "f119f7a7-c1a8-44a6-a678-0e13c46294fd"},
  "code": {"code": "34390-5", "codeSystem": "2.16.840.1.113883.6.1", "displayName": "HUMAN OTC DRUG LABEL"},
  "effectiveTime": {"value": "20090617"}
}
```

### 2. Author Organization (`author` → `organizations`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `author.assignedEntity.representedOrganization.name` | `name` | Organization name |
| `author.assignedEntity.representedOrganization.id.root` | `id_root` | Organization root ID |
| `author.assignedEntity.representedOrganization.id.extension` | `id_extension` | Organization extension ID |

**Example:**
```json
"author": {
  "assignedEntity": {
    "representedOrganization": {
      "id": {"root": "1.3.6.1.4.1.519.1", "extension": "176792315"},
      "name": "Insight Pharmaceuticals"
    }
  }
}
```

### 3. Products (`manufactured_products[]` → `products`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `code.code` | `product_code` | Product NDC code |
| `code.codeSystem` | `product_code_system` | Product code system |
| `name.text` | `name_text` | Plain text product name |
| `name.html` | `name_html` | HTML formatted name |
| `name.suffix` | `name_suffix` | Name suffix (e.g., "Kids") |
| `formCode.code` | `form_code` | Dosage form code |
| `formCode.codeSystem` | `form_code_system` | Form code system |
| `formCode.displayName` | `form_display_name` | Human readable form |
| `consumedIn.substanceAdministration.routeCode.code` | `route_code` | Administration route code |
| `consumedIn.substanceAdministration.routeCode.codeSystem` | `route_code_system` | Route code system |
| `consumedIn.substanceAdministration.routeCode.displayName` | `route_display_name` | Human readable route |

**Example:**
```json
"manufactured_products": [{
  "code": {"code": "63736-044", "codeSystem": "2.16.840.1.113883.6.69"},
  "name": {"text": "Bonine Kids", "suffix": "Kids"},
  "formCode": {"code": "C42893", "displayName": "TABLET, CHEWABLE"},
  "consumedIn": {
    "substanceAdministration": {
      "routeCode": {"code": "C38288", "displayName": "ORAL"}
    }
  }
}]
```

### 4. Generic Medicines (`genericMedicine[]` → `generic_medicines`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| Array items | `generic_name` | Each string becomes a separate row |
| (Foreign Key) | `product_id` | References products.id |

**Example:**
```json
"genericMedicine": ["Chlorcyclizine Hydrochloride"]
```

### 5. Ingredient Substances (`ingredients[].ingredientSubstance` → `ingredient_substances`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `code.code` | `substance_code` | UNII or substance code |
| `code.codeSystem` | `substance_code_system` | Code system (usually FDA UNII) |
| `name` | `substance_name` | Substance name |

**Example:**
```json
"ingredientSubstance": {
  "code": {"code": "NPB7A7874U", "codeSystem": "2.16.840.1.113883.4.9"},
  "name": "Chlorcyclizine Hydrochloride"
}
```

### 6. Active Moieties (`activeMoiety[]` → `active_moieties`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `code.code` | `moiety_code` | Active moiety UNII code |
| `code.codeSystem` | `moiety_code_system` | Code system |
| `name` | `moiety_name` | Active moiety name |
| (Foreign Key) | `ingredient_substance_id` | References ingredient_substances.id |

**Example:**
```json
"activeMoiety": [{
  "code": {"code": "M26C4IP44P", "codeSystem": "2.16.840.1.113883.4.9"},
  "name": "Chlorcyclizine"
}]
```

### 7. Ingredients (`ingredients[]` → `ingredients`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `classCode` | `class_code` | "ACTIM" (active) or "IACT" (inactive) |
| `quantity.numerator.value` | `quantity_numerator_value` | Amount value |
| `quantity.numerator.unit` | `quantity_numerator_unit` | Unit (mg, g, etc.) |
| `quantity.denominator.value` | `quantity_denominator_value` | Per unit value |
| `quantity.denominator.unit` | `quantity_denominator_unit` | Per unit type |
| (Foreign Key) | `product_id` | References products.id |
| (Foreign Key) | `ingredient_substance_id` | References ingredient_substances.id |

**Example:**
```json
"ingredients": [{
  "classCode": "ACTIM",
  "quantity": {
    "numerator": {"value": "25", "unit": "mg"},
    "denominator": {"value": "1", "unit": "1"}
  }
}]
```

### 8. Packaging (`packaging[]` → `packaging_configurations`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `quantity.numerator.value` | `quantity_numerator_value` | Package quantity |
| `quantity.numerator.unit` | `quantity_numerator_unit` | Package unit |
| `quantity.denominator.value` | `quantity_denominator_value` | Per container value |
| `quantity.numerator.translation.code` | `translation_code` | Translation code |
| `quantity.numerator.translation.codeSystem` | `translation_code_system` | Translation code system |
| `quantity.numerator.translation.displayName` | `translation_display_name` | Translation display name |
| `containerPackagedProduct.code` | `container_code` | Container NDC code |
| `containerPackagedProduct.codeSystem` | `container_code_system` | Container code system |
| `containerPackagedProduct.formCode.code` | `container_form_code` | Container form code |
| `containerPackagedProduct.formCode.codeSystem` | `container_form_code_system` | Container form code system |
| `containerPackagedProduct.formCode.displayName` | `container_form_display_name` | Container form name |
| (Foreign Key) | `product_id` | References products.id |

**Example:**
```json
"packaging": [{
  "quantity": {
    "numerator": {
      "value": "8", "unit": "1",
      "translation": {"code": "C48542", "displayName": "TABLET"}
    },
    "denominator": {"value": "1"}
  },
  "containerPackagedProduct": {
    "code": "63736-044-08",
    "formCode": {"code": "C43178", "displayName": "BOX"}
  }
}]
```

### 9. Approvals (`subjectOf.approvals[]` → `approvals`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `id.root` | `approval_id_root` | Approval identifier root |
| `id.extension` | `approval_id_extension` | Approval identifier extension |
| `code.code` | `approval_code` | Approval type code |
| `code.codeSystem` | `approval_code_system` | Approval code system |
| `code.displayName` | `approval_display_name` | Approval type name |
| `author.territorialAuthority.territory.code` | `territory_code` | Territory code (USA, etc.) |
| `author.territorialAuthority.territory.codeSystem` | `territory_code_system` | Territory code system |
| (Foreign Key) | `product_id` | References products.id |

**Example:**
```json
"approvals": [{
  "id": {"root": "2.16.840.1.113883.3.149", "extension": "part336"},
  "code": {"code": "C73603", "displayName": "OTC monograph final"},
  "author": {
    "territorialAuthority": {
      "territory": {"code": "USA", "codeSystem": "2.16.840.1.113883.5.28"}
    }
  }
}]
```

### 10. Marketing Acts (`subjectOf.marketingActs[]` → `marketing_acts`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `code.code` | `marketing_code` | Marketing act code |
| `code.codeSystem` | `marketing_code_system` | Marketing code system |
| `statusCode.code` | `status_code` | Marketing status (active, etc.) |
| `effectiveTime.low.value` | `effective_time_low` | Marketing start date |
| `effectiveTime.value` | `effective_time_high` | Marketing end date (if present) |
| (Foreign Key) | `product_id` | References products.id |

**Example:**
```json
"marketingActs": [{
  "code": {"code": "C53292", "codeSystem": "2.16.840.1.113883.3.26.1.1"},
  "statusCode": {"code": "active"},
  "effectiveTime": {"low": {"value": "20090608"}}
}]
```

### 11. Product Characteristics (`subjectOf.characteristics[]` → `product_characteristics`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `code.code` | `characteristic_code` | Characteristic type (SPLCOLOR, SPLSHAPE, etc.) |
| `code.codeSystem` | `characteristic_code_system` | Characteristic code system |
| `value.xsi_type` | `value_type` | Value type (CE, INT, PQ, ST) |
| `value.code` | `value_code` | Coded value |
| `value.codeSystem` | `value_code_system` | Value code system |
| `value.displayName` | `value_display_name` | Value display name |
| `value.text` | `value_text` | Text value |
| `value.value` | `value_numeric` | Numeric value |
| `value.unit` | `value_unit` | Unit for numeric values |
| (Foreign Key) | `product_id` | References products.id |

**Example:**
```json
"characteristics": [
  {
    "code": {"code": "SPLCOLOR"},
    "value": {"xsi_type": "CE", "code": "C48328", "displayName": "PINK"}
  },
  {
    "code": {"code": "SPLSIZE"},
    "value": {"xsi_type": "PQ", "value": "14", "unit": "mm"}
  },
  {
    "code": {"code": "SPLIMPRINT"},
    "value": {"xsi_type": "ST", "text": "Bonine"}
  }
]
```

### 12. Observation Media (`observation_media[]` → `observation_media`)

| Python Field | Database Field | Notes |
|-------------|---------------|-------|
| `ID` | `media_id` | Media identifier |
| `text` | `media_text` | Media description |
| `value.mediaType` | `media_type` | MIME type (image/jpeg, etc.) |
| `value.reference` | `media_reference` | File reference |
| (Foreign Key) | `document_id` | References documents.id |

**Example:**
```json
"observation_media": [{
  "ID": "MM1",
  "text": "PRINCIPAL DISPLAY PANEL - Box of 8 Tablets",
  "value": {
    "xsi_type": "ED",
    "mediaType": "image/jpeg",
    "reference": "bonine-01.jpg"
  }
}]
```

## Excluded Fields

The following fields from the Python dataclass are **NOT** stored in the database as they contain unstructured text:

- `free_text_sections[]` - All clinical text content (warnings, indications, directions, etc.)
- `notes` - Conversion metadata
- Most HTML content except where specifically needed for display

## Foreign Key Relationships

1. `documents.author_organization_id` → `organizations.id`
2. `products.document_id` → `documents.id`
3. `generic_medicines.product_id` → `products.id`
4. `ingredients.product_id` → `products.id`
5. `ingredients.ingredient_substance_id` → `ingredient_substances.id`
6. `active_moieties.ingredient_substance_id` → `ingredient_substances.id`
7. `packaging_configurations.product_id` → `products.id`
8. `approvals.product_id` → `products.id`
9. `marketing_acts.product_id` → `products.id`
10. `product_characteristics.product_id` → `products.id`
11. `observation_media.document_id` → `documents.id`

## Data Processing Notes

1. **Normalization**: Ingredient substances are normalized to avoid duplication
2. **Code Systems**: Common code system OIDs are stored in `code_systems` reference table
3. **Date Formats**: HL7 dates (YYYYMMDD) are converted to PostgreSQL DATE format
4. **Quantities**: Numeric values are stored as DECIMAL for precision
5. **Optional Fields**: All non-required fields allow NULL values
6. **Indexing**: Key fields are indexed for query performance