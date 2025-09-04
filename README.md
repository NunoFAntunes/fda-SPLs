FDA Structured Product Label (SPL) Summarization & Extraction
📌 Overview

This project demonstrates how to build a GenAI + NLP pipeline for healthcare documents using the FDA Structured Product Labels (SPLs) dataset from DailyMed
.

The pipeline ingests SPL XML files, extracts and normalizes structured data (e.g., drug names, NDCs, manufacturer info, indications, dosage, warnings), and uses Large Language Models (LLMs) to generate patient-friendly summaries at multiple reading levels.

This project is designed to showcase skills relevant to GenAI/LLM roles in the healthcare sector:

Document ingestion & parsing

Information extraction

Summarization & simplification

Building structured knowledge APIs

Search & retrieval (optional)

🎯 Goals

Deterministic Extraction

Parse SPL XML into structured JSON (or database objects).

Normalize identifiers (NDCs, dates, dosage units).

Summarization & Simplification

Use an LLM to summarize key sections (Indications, Dosage, Warnings, Side Effects).

Generate plain-language outputs at grade 6 and grade 9 reading levels.

APIs & Frontend

Provide a REST API for search and label access.

Build a simple React UI to view structured info and summaries.

Demonstrate Practical Healthcare NLP

End-to-end ingestion pipeline

Hybrid approach: deterministic parsing + LLM for interpretation

Factuality and readability checks

🏗️ System Architecture
                ┌───────────────────┐
                │  DailyMed SPL XML │
                └─────────┬─────────┘
                          │ Ingestion (nightly job)
                          ▼
                ┌───────────────────────┐
                │ Raw XML Storage (S3)  │
                └─────────┬─────────────┘
                          │ Parsing & Normalization
                          ▼
                ┌───────────────────────┐
                │ Structured JSON/DB     │
                │  - SPL ID, Version     │
                │  - Brand/Generic Name  │
                │  - NDCs, Manufacturer  │
                │  - Sections (text)     │
                └─────────┬─────────────┘
                          │ Summarization & Simplification
                          ▼
                ┌───────────────────────┐
                │ LLM Outputs            │
                │  - Bullet summaries    │
                │  - Plain-language text │
                │  - Reading levels      │
                └─────────┬─────────────┘
                          │ API (FastAPI)
                          ▼
                ┌───────────────────────┐
                │ Web UI (React)        │
                │  - Search & Browse    │
                │  - Structured view    │
                │  - Patient summary    │
                └───────────────────────┘

🧩 Components
1. Ingestion

Fetch SPL XML bulk files from DailyMed.

Store raw files (local or S3).

Track versions with hash checks.

2. Parsing & Normalization (Deterministic)

Tech: Python (lxml, pydantic)

Extract:

SPL ID, set ID, version, effective date

Brand name, generic name, manufacturer

NDC codes and packaging

Section texts (Indications, Dosage, Contraindications, Warnings, Adverse Reactions, Drug Interactions, Storage, etc.)

Normalize:

Dates → ISO-8601

Dosage units → UCUM standard

Drug names → RxNorm (optional)

Store in Postgres (JSONB fields + search index).

**Implementation Phases:**

**Phase 1: Core Data Models & Parser Infrastructure** ✅ **COMPLETED**
- ✅ Create comprehensive data models for structured SPL data with proper typing (`parse/models.py`)
- ✅ Build XML namespace handler and utilities for HL7 v3 structure (`parse/base_parser.py`)
- ✅ Implement base parser classes with error handling and validation
- ✅ Create validation system with comprehensive error reporting (`parse/validators.py`)
- ✅ Add section type mapping using LOINC codes
- ✅ Build text extraction utilities for complex SPL sections

**Phase 2: Data Extraction Pipeline** ✅ **COMPLETED**
- ✅ Extract document metadata: SPL ID, set ID, version, dates
- ✅ Parse manufacturer/labeler information from author sections
- ✅ Extract product data: brand names, generic names, strength, dosage form
- ✅ Collect NDC codes and packaging information with quantities
- ✅ Parse all clinical sections using LOINC codes as identifiers
- ✅ Implement specialized parsers for different SPL components
- ✅ Add batch processing with parallel execution capabilities
- ✅ Create comprehensive validation and error reporting system
- ✅ Build end-to-end extraction pipeline with monitoring

### Phase 2 Implementation Roadmap

**Architecture Overview:**
The extraction pipeline follows a modular design with specialized parsers for different SPL components, coordinated through a main document parser and factory pattern.

**Step 1: Core Document Parser** (`parse/spl_document_parser.py`)
- Main entry point for SPL XML parsing
- Extract document metadata (ID, set ID, version, effective date)
- Parse document-level coded concepts (document type, language)
- Coordinate author/organization parsing (reuse existing parsers)
- Orchestrate section discovery and routing
- Assemble final SPLDocument with comprehensive validation

**Step 2: Product Information Extraction** (`parse/product_parser.py`)
- Target: SPL Listing sections (LOINC code 48780-1)
- Extract manufactured product details:
  - Product names (brand + suffix handling)
  - NDC codes with validation
  - Dosage forms and strengths
  - Generic medicine mappings
- Parse packaging information with quantities
- Extract marketing status and approval data
- Handle route of administration codes

**Step 3: Ingredient Processing** (`parse/ingredient_parser.py`)
- Parse complex ingredient hierarchies (active vs inactive)
- Extract substance information:
  - UNII codes and substance names
  - Quantity structures (numerator/denominator)
  - Unit standardization
- Handle active moiety relationships
- Validate against known substance databases
- Support nested ingredient structures

**Step 4: Clinical Section Processing** (`parse/clinical_section_parser.py`)
- Handle text-heavy sections (warnings, indications, dosage)
- Extract and clean HTML-like content from `<text>` elements:
  - Preserve semantic formatting (lists, emphasis)
  - Remove XML markup while maintaining readability
  - Handle nested content structures
- Parse media references and observational data
- Map sections using LOINC codes to semantic types

**Step 5: Section Routing & Factory** (`parse/section_parser.py`, `parse/parser_factory.py`)
- Implement section discovery and type identification
- Route sections to appropriate specialized parsers
- Handle recursive parsing of nested subsections
- Factory pattern for parser instantiation and management
- Section-type to parser mapping configuration

**Step 6: Batch Processing Infrastructure** (`parse/batch_processor.py`)
- File discovery and iteration capabilities
- Parallel processing with configurable worker threads
- Progress tracking and reporting
- Error recovery and retry mechanisms
- Memory-efficient processing for large datasets

**Step 7: Pipeline Orchestration** (`parse/extraction_pipeline.py`)
- End-to-end workflow coordination
- Configuration management for different SPL types
- Integration with existing validation system
- Comprehensive error handling and reporting
- Performance monitoring and extraction metrics

**Technical Challenges & Solutions:**

*XML Complexity:*
- HL7 v3 namespace handling throughout document hierarchy
- Multiple schema variations (OTC vs Rx vs Biologics)
- Solution: Robust namespace utilities and schema-agnostic parsing

*Data Extraction:*
- 50+ different LOINC section types requiring specialized handling
- Complex quantity structures with multiple unit systems
- Solution: Extensible parser factory with type-specific handlers

*Performance & Scalability:*
- Large XML files (1MB+ per document)
- Batch processing of 100,000+ SPL documents
- Solution: Streaming XML parsing, memory optimization, parallel processing

**Data Flow Architecture:**
```
SPL XML File
    ↓
SPLDocumentParser (orchestrator)
    ├── Document metadata extraction
    ├── Author/Organization parsing (existing)
    ├── Section discovery & routing
    │   ├── SPL Listing → ProductParser → IngredientParser
    │   ├── Clinical Sections → ClinicalSectionParser  
    │   └── Generic Sections → SectionParser
    ├── Validation integration
    └── SPLDocument assembly
    ↓
Validated SPLDocument object → Phase 3
```

**Success Metrics for Phase 2:**
- Parse 95%+ of SPL documents without fatal errors
- Extract structured data from all major section types
- Handle edge cases in ingredient and product parsing
- Process documents in <2 seconds per file average
- Maintain detailed error reporting for quality assurance

### Phase 2 Implementation Results ✅

**Components Delivered:**
- `parse/spl_document_parser.py` - Main orchestrator for SPL document parsing
- `parse/product_parser.py` - Extracts manufactured product details, NDC codes, packaging
- `parse/ingredient_parser.py` - Handles complex ingredient hierarchies with UNII validation
- `parse/clinical_section_parser.py` - Processes clinical text with section-specific formatting
- `parse/section_parser.py` - Routes sections to appropriate specialized parsers
- `parse/parser_factory.py` - Factory pattern with configuration management
- `parse/batch_processor.py` - Parallel processing infrastructure with progress tracking
- `parse/extraction_pipeline.py` - End-to-end workflow orchestration with monitoring

**Test Results:** 8/9 tests passed (88.9% success rate)
- ✅ Document Parser - Successfully parses SPL documents
- ✅ Section Analysis - Analyzes section distribution and metrics
- ✅ Product & Ingredient Parsing - Extracts product and ingredient data
- ✅ Clinical Text Processing - Processes clinical sections with text cleaning
- ✅ Validation System - Comprehensive document validation with detailed reporting
- ✅ Batch Processing - 100% success rate with parallel execution
- ✅ Extraction Pipeline - Complete end-to-end workflow orchestration
- ✅ Integration Workflow - Full integration testing with output generation

**Performance Achieved:**
- Processing speed: ~2ms per document
- Batch success rate: 100% on test dataset
- Memory efficient streaming XML processing
- Configurable parallel processing (thread/process-based)
- Comprehensive error handling and recovery
- Structured output formats (JSON, JSONL, CSV)

**Phase 3: Data Normalization** ⏳ **PENDING**
- Standardize dates to ISO-8601 format
- Normalize dosage units to UCUM standard where possible
- Clean and structure text content (remove XML markup, preserve formatting)
- Validate NDC format and structure

**Phase 4: Database Integration** ⏳ **PENDING**
- Design PostgreSQL schema with JSONB fields for flexible section storage
- Create indexes for search performance (full-text, NDC lookups)
- Implement batch processing for large SPL datasets
- Add data validation and duplicate detection

**Phase 5: Quality Assurance** ⏳ **PENDING**
- Build comprehensive test suite with edge cases
- Create parsing coverage metrics to track extraction success rates
- Implement error logging and recovery mechanisms
- Add performance monitoring for large-scale processing

3. Summarization & Simplification (LLM)

Tech: GPT-4o-mini / Llama-3.1-8B-Instruct (via vLLM)

Guardrails:

Provide source text chunks only.

Enforce JSON schema (retry on invalid output).

Validate factual consistency via substring checks.

Outputs:

{
  "indications_bullets": ["Treats high blood pressure", "Prevents heart attacks"],
  "dosage_summary": "Take 1 tablet daily with water.",
  "important_warnings": ["Do not use during pregnancy"],
  "common_side_effects": ["Dizziness", "Headache"],
  "when_to_seek_help": ["Chest pain", "Difficulty breathing"],
  "reading_level": "grade6"
}

4. API Layer

Tech: FastAPI + Postgres

Endpoints:

/search?q=drugname → search results

/labels/{spl_id} → normalized JSON

/labels/{spl_id}/summary?level=grade6|grade9 → LLM summary

/ndc/{ndc} → reverse lookup

5. Frontend

Tech: React + Tailwind (or Next.js)

Features:

Search by brand/generic name or NDC

Label detail page with tabs: Overview, Safety, Dosing, Packages

Toggle: Patient summary (grade 6 vs grade 9) vs Professional view (raw text)

Download JSON

📊 Evaluation

Parsing coverage: % of labels with extracted sections

Readability: Flesch-Kincaid grade level check (≤6, ≤9)

Factuality: Ensure summary claims match original section text

Latency: P95 response time for summary endpoint

⚙️ Tech Stack

Backend: Python, FastAPI, Pydantic, lxml, Postgres, Redis (for caching)

LLM Layer: GPT-4o-mini / Llama-3.1 (via vLLM), JSON guardrails

Search: Postgres full-text or Elasticsearch/OpenSearch

Frontend: React + Tailwind

Infra: Docker Compose, optional S3/Minio for raw XML storage

🚀 Roadmap

MVP

Parse SPL XML → JSON

Store in Postgres

Expose API for search + details

LLM Integration

Summarize sections

Enforce reading levels & JSON schema

Frontend

Search + drug detail page

Toggle between professional vs patient view

Enhancements

Version diffs (track label changes)

Contraindication highlighter

FHIR MedicationKnowledge export

📚 References

DailyMed Bulk SPL Data

SPL Implementation Guide (HL7)

RxNorm API

UCUM Units

🔑 Key Takeaway:
This project shows how to combine deterministic parsing of structured healthcare documents with LLM-powered summarization to make complex medical data accessible, trustworthy, and usable in real applications.