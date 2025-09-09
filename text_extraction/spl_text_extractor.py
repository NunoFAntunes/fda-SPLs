#!/usr/bin/env python3
"""
FDA SPL Text Extraction for Embeddings
Extract and chunk text content from parsed JSON files for vector embeddings.
"""

import json
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TextChunk:
    """Represents a text chunk with metadata for embeddings"""
    document_id: str
    section_type: str
    section_title: str
    text_content: str
    chunk_index: int
    total_chunks: int
    product_name: Optional[str] = None
    manufacturer: Optional[str] = None
    effective_date: Optional[str] = None
    source_file: Optional[str] = None
    loinc_code: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'document_id': self.document_id,
            'section_type': self.section_type,
            'section_title': self.section_title,
            'text_content': self.text_content,
            'chunk_index': self.chunk_index,
            'total_chunks': self.total_chunks,
            'product_name': self.product_name,
            'manufacturer': self.manufacturer,
            'effective_date': self.effective_date,
            'source_file': self.source_file,
            'loinc_code': self.loinc_code
        }

class SPLTextExtractor:
    """Extracts text content from SPL JSON files for embeddings"""
    
    # LOINC codes for medical sections we want to prioritize
    MEDICAL_SECTIONS = {
        '34089-3': 'DESCRIPTION',
        '34090-1': 'CLINICAL PHARMACOLOGY', 
        '34067-9': 'INDICATIONS AND USAGE',
        '34070-3': 'CONTRAINDICATIONS',
        '43685-7': 'WARNINGS AND PRECAUTIONS',
        '34084-4': 'ADVERSE REACTIONS',
        '34068-7': 'DOSAGE AND ADMINISTRATION',
        '34088-5': 'OVERDOSAGE',
        '50570-1': 'OTC - WHEN USING THIS PRODUCT',
        '50569-3': 'OTC - ASK DOCTOR',
        '50568-5': 'OTC - ASK DOCTOR BEFORE USE',
        '50567-7': 'OTC - DO NOT USE',
        '50566-9': 'OTC - STOP USE',
        '42232-9': 'PRECAUTIONS',
        '54433-8': 'USER SAFETY WARNINGS',
        '44425-7': 'STORAGE AND HANDLING',
        '60560-0': 'INTENDED USE OF THE DEVICE'
    }
    
    def __init__(self, max_chunk_size: int = 500):
        """
        Initialize the text extractor.
        
        Args:
            max_chunk_size: Maximum number of words per chunk for embeddings
        """
        self.max_chunk_size = max_chunk_size
    
    def extract_document_metadata(self, spl_data: Dict) -> Dict[str, str]:
        """Extract key metadata from the SPL document"""
        metadata = {}
        
        # Document ID
        if 'document' in spl_data and 'id' in spl_data['document']:
            metadata['document_id'] = spl_data['document']['id'].get('root', '')
        
        # Product name (take first manufactured product)
        if 'manufactured_products' in spl_data and spl_data['manufactured_products']:
            product = spl_data['manufactured_products'][0]
            if 'name' in product:
                metadata['product_name'] = product['name'].get('text', '')
        
        # Manufacturer
        if 'author' in spl_data:
            author = spl_data['author']
            if 'assignedEntity' in author and 'representedOrganization' in author['assignedEntity']:
                org = author['assignedEntity']['representedOrganization']
                metadata['manufacturer'] = org.get('name', '')
        
        # Effective date
        if 'document' in spl_data and 'effectiveTime' in spl_data['document']:
            metadata['effective_date'] = spl_data['document']['effectiveTime'].get('value', '')
        
        # Source file
        if 'source_file' in spl_data:
            metadata['source_file'] = spl_data['source_file'].get('path', '')
        
        return metadata
    
    def chunk_text(self, text: str, section_type: str) -> List[str]:
        """
        Split text into chunks suitable for embeddings.
        
        Args:
            text: The text to chunk
            section_type: Type of section for context-aware chunking
            
        Returns:
            List of text chunks
        """
        if not text or not text.strip():
            return []
        
        words = text.split()
        if len(words) <= self.max_chunk_size:
            return [text]
        
        chunks = []
        current_chunk = []
        
        for word in words:
            current_chunk.append(word)
            
            # Check if we should end this chunk
            if len(current_chunk) >= self.max_chunk_size:
                # Try to end at sentence boundary
                chunk_text = ' '.join(current_chunk)
                if '.' in chunk_text[-50:]:  # Look for sentence end in last 50 chars
                    last_period = chunk_text.rfind('.')
                    if last_period > len(chunk_text) - 50:
                        # Split at the sentence boundary
                        chunks.append(chunk_text[:last_period + 1].strip())
                        remaining_words = chunk_text[last_period + 1:].strip().split()
                        current_chunk = [w for w in remaining_words if w]
                        continue
                
                # No good sentence boundary, just split at word boundary
                chunks.append(chunk_text.strip())
                current_chunk = []
        
        # Add remaining words
        if current_chunk:
            chunks.append(' '.join(current_chunk).strip())
        
        return chunks
    
    def extract_text_chunks(self, json_file_path: str) -> List[TextChunk]:
        """
        Extract all text chunks from a single SPL JSON file.
        
        Args:
            json_file_path: Path to the JSON file
            
        Returns:
            List of TextChunk objects
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                spl_data = json.load(f)
        except Exception as e:
            logger.error(f"Error loading JSON file {json_file_path}: {e}")
            return []
        
        # Extract metadata
        metadata = self.extract_document_metadata(spl_data)
        document_id = metadata.get('document_id', '')
        
        if not document_id:
            logger.warning(f"No document ID found in {json_file_path}")
            return []
        
        chunks = []
        
        # Extract from free_text_sections
        if 'free_text_sections' in spl_data:
            for section in spl_data['free_text_sections']:
                section_code = section.get('code', {}).get('code', '')
                section_title = section.get('title', '')
                text_content = section.get('text_plain', '') or section.get('text_html', '')
                
                if not text_content or not text_content.strip():
                    continue
                
                # Determine section type
                section_type = self.MEDICAL_SECTIONS.get(section_code, 'OTHER')
                
                # Chunk the text
                text_chunks = self.chunk_text(text_content, section_type)
                
                # Create TextChunk objects
                for i, chunk_text in enumerate(text_chunks):
                    chunk = TextChunk(
                        document_id=document_id,
                        section_type=section_type,
                        section_title=section_title,
                        text_content=chunk_text,
                        chunk_index=i,
                        total_chunks=len(text_chunks),
                        product_name=metadata.get('product_name'),
                        manufacturer=metadata.get('manufacturer'),
                        effective_date=metadata.get('effective_date'),
                        source_file=metadata.get('source_file'),
                        loinc_code=section_code
                    )
                    chunks.append(chunk)
        
        # Also extract product descriptions from manufactured_products
        if 'manufactured_products' in spl_data:
            for i, product in enumerate(spl_data['manufactured_products']):
                product_name = product.get('name', {}).get('text', '')
                
                # Combine product information
                product_info = []
                if product_name:
                    product_info.append(f"Product: {product_name}")
                
                # Add dosage form
                if 'formCode' in product and 'displayName' in product['formCode']:
                    product_info.append(f"Form: {product['formCode']['displayName']}")
                
                # Add route
                if 'consumedIn' in product and 'substanceAdministration' in product['consumedIn']:
                    route = product['consumedIn']['substanceAdministration'].get('routeCode', {})
                    if 'displayName' in route:
                        product_info.append(f"Route: {route['displayName']}")
                
                # Add active ingredients
                active_ingredients = []
                if 'ingredients' in product:
                    for ingredient in product['ingredients']:
                        if ingredient.get('classCode') in ['ACTIM', 'ACTIB']:  # Active ingredients
                            ing_name = ingredient.get('ingredientSubstance', {}).get('name', '')
                            if ing_name:
                                quantity = ingredient.get('quantity', {})
                                if quantity:
                                    num = quantity.get('numerator', {})
                                    if num.get('value') and num.get('unit'):
                                        active_ingredients.append(f"{ing_name} {num['value']}{num['unit']}")
                                    else:
                                        active_ingredients.append(ing_name)
                
                if active_ingredients:
                    product_info.append(f"Active ingredients: {', '.join(active_ingredients)}")
                
                if product_info:
                    product_text = '. '.join(product_info) + '.'
                    chunk = TextChunk(
                        document_id=document_id,
                        section_type='PRODUCT_INFO',
                        section_title=f'Product Information - {product_name}',
                        text_content=product_text,
                        chunk_index=0,
                        total_chunks=1,
                        product_name=metadata.get('product_name'),
                        manufacturer=metadata.get('manufacturer'),
                        effective_date=metadata.get('effective_date'),
                        source_file=metadata.get('source_file'),
                        loinc_code='PRODUCT'
                    )
                    chunks.append(chunk)
        
        logger.info(f"Extracted {len(chunks)} text chunks from {json_file_path}")
        return chunks
    
    def process_directory(self, input_dir: str, output_dir: str) -> None:
        """
        Process all JSON files in a directory and save extracted chunks.
        
        Args:
            input_dir: Directory containing JSON files
            output_dir: Directory to save extracted chunks
        """
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        json_files = list(input_path.glob("**/*.json"))
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        all_chunks = []
        processed_count = 0
        
        for json_file in json_files:
            chunks = self.extract_text_chunks(str(json_file))
            all_chunks.extend(chunks)
            processed_count += 1
            
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count}/{len(json_files)} files")
        
        # Save all chunks
        chunks_output_file = output_path / "extracted_text_chunks.jsonl"
        with open(chunks_output_file, 'w', encoding='utf-8') as f:
            for chunk in all_chunks:
                f.write(json.dumps(chunk.to_dict()) + '\n')
        
        # Save summary statistics
        stats = self._generate_stats(all_chunks)
        stats_file = output_path / "extraction_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Extraction complete. Saved {len(all_chunks)} chunks to {chunks_output_file}")
        logger.info(f"Statistics saved to {stats_file}")
    
    def _generate_stats(self, chunks: List[TextChunk]) -> Dict:
        """Generate extraction statistics"""
        section_counts = {}
        document_counts = {}
        total_words = 0
        
        for chunk in chunks:
            # Count by section type
            section_counts[chunk.section_type] = section_counts.get(chunk.section_type, 0) + 1
            
            # Count by document
            document_counts[chunk.document_id] = document_counts.get(chunk.document_id, 0) + 1
            
            # Count words
            total_words += len(chunk.text_content.split())
        
        return {
            'total_chunks': len(chunks),
            'total_documents': len(document_counts),
            'total_words': total_words,
            'avg_words_per_chunk': total_words / len(chunks) if chunks else 0,
            'section_type_distribution': section_counts,
            'chunks_per_document': {
                'min': min(document_counts.values()) if document_counts else 0,
                'max': max(document_counts.values()) if document_counts else 0,
                'avg': sum(document_counts.values()) / len(document_counts) if document_counts else 0
            }
        }


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract text from SPL JSON files for embeddings')
    parser.add_argument('input_dir', help='Directory containing JSON files')
    parser.add_argument('output_dir', help='Directory to save extracted chunks')
    parser.add_argument('--chunk-size', type=int, default=500, 
                       help='Maximum words per chunk (default: 500)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    extractor = SPLTextExtractor(max_chunk_size=args.chunk_size)
    extractor.process_directory(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()