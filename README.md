# FDA Structured Product Label (SPL) Summarization & Extraction

## Overview

This project creates an LLM-powered system to answer questions about medication using the FDA Structured Product Labels (SPLs) dataset from DailyMed. The dataset consists of XML files containing comprehensive medication information, including both structured fields (medication names, ingredients, NDCs) and unstructured content (contraindications, precautions, warnings).

**Example data:** See `example.xml` in the project root for a sample SPL file.

## Project Goals

This project demonstrates how to build a GenAI + NLP pipeline for healthcare documents, showcasing skills relevant to GenAI/LLM roles in the healthcare sector:

- **Document ingestion & parsing** - Processing XML SPL files
- **Information extraction** - Extracting structured and unstructured data
- **Data normalization** - Standardizing medication information
- **Summarization & simplification** - Generating patient-friendly content
- **Building structured knowledge APIs** - Creating queryable interfaces
- **Search & retrieval** - Implementing hybrid search capabilities

## Architecture & Data Strategy

### Data Types and Storage Approach

**Structured Data** (NDC codes, active ingredients, dosage forms, manufacturer info, colors, imprints)
- Requirements: Precise queryability (e.g., "Show me all tablets with polythiazide")
- Solution: Relational database (PostgreSQL)

**Unstructured Data** (indications, contraindications, warnings, precautions, adverse reactions)
- Requirements: Semantic search and contextual retrieval
- Solution: Vector database with embeddings (RAG)

**Lexical/Keyword Search** (drug codes, imprint codes, ingredient names)
- Requirements: Exact text matching for regulatory data
- Solution: Elasticsearch/OpenSearch for flexible keyword indexing

### Hybrid Solution

**RAG + Relational DB + Elasticsearch**

This three-tier approach provides:
- **Precision**: Structured SQL queries for exact data retrieval
- **Semantic Context**: RAG for understanding complex medical queries
- **Robust Keyword Lookup**: Elasticsearch for regulatory code searches

*Why not RAG alone?* LLMs can hallucinate without structured data validation.
*Why not just RAG + DB?* Missing flexible keyword search critical for medical lookups.