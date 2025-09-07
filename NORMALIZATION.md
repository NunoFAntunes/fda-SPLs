Phase 3 Implementation Steps

  Step 1: Extend Data Models

  - Add NormalizedSPLDocument class extending SPLDocument
  - Add normalized fields for extracted clinical knowledge
  - Add confidence scores and processing metadata

  Step 2: Basic Text Processing Infrastructure

  - Create TextCleaner to remove XML markup and normalize whitespace
  - Create DateNormalizer for ISO-8601 standardization
  - Create UnitNormalizer for basic UCUM unit conversion
  - Create NDCValidator for NDC format validation

  Step 3: Simple LLM-Based Knowledge Extractors

  - Create AdverseEffectExtractor using LLM prompts to identify side
  effects
  - Create DosingExtractor to extract dose/frequency/duration from
  text
  - Create ContraindicationExtractor for population restrictions
  - Create WarningClassifier to categorize warning severity

  Step 4: Normalization Pipeline

  - Create Phase3Processor main class to orchestrate processing
  - Process each SPLSection.text_content field through extractors
  - Combine normalized data with original SPLDocument
  - Generate confidence scores for extractions

  Step 5: LLM Integration Layer

  - Create LLMService wrapper for OpenAI/Anthropic APIs
  - Define structured prompts for each extraction task
  - Implement retry logic and error handling
  - Add JSON schema validation for LLM outputs

  Step 6: Quality Assurance

  - Create ExtractionValidator to check output quality
  - Add logging for low-confidence extractions
  - Create test cases with sample SPL documents
  - Add processing metrics and error reporting

  Step 7: Integration & Output

  - Modify existing pipeline to call Phase 3 after Phase 2
  - Output NormalizedSPLDocument objects as JSON/JSONL
  - Ensure compatibility with Phase 4 database schema
  - Add configuration for different processing modes

  Step 8: Testing & Validation

  - Test on sample SPL documents from different categories
  - Validate extraction accuracy on known examples
  - Performance testing for processing speed
  - Integration testing with full pipeline

  Key Simplifications:
  - Use LLMs directly instead of complex ML models
  - Start with basic rule-based normalization
  - Focus on most common clinical sections first
  - Incremental implementation - one extractor at a time