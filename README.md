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

**Phase 1: Core Data Models & Parser Infrastructure**
- Create Pydantic models for structured SPL data with proper typing
- Build XML namespace handler for HL7 v3 structure
- Implement base parser class with error handling and validation

**Phase 2: Data Extraction Pipeline**
- Extract document metadata: SPL ID, set ID, version, dates
- Parse manufacturer/labeler information from author sections
- Extract product data: brand names, generic names, strength, dosage form
- Collect NDC codes and packaging information with quantities
- Parse all clinical sections using LOINC codes as identifiers

**Phase 3: Data Normalization**
- Standardize dates to ISO-8601 format
- Normalize dosage units to UCUM standard where possible
- Clean and structure text content (remove XML markup, preserve formatting)
- Validate NDC format and structure

**Phase 4: Database Integration**
- Design PostgreSQL schema with JSONB fields for flexible section storage
- Create indexes for search performance (full-text, NDC lookups)
- Implement batch processing for large SPL datasets
- Add data validation and duplicate detection

**Phase 5: Quality Assurance**
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