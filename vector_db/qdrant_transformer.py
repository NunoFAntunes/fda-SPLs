#!/usr/bin/env python3
"""
Transform extracted text chunks to Qdrant schema format
Converts the JSONL output from text extraction to Qdrant-compatible format
"""

import json
import uuid
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QdrantTransformer:
    """Transform text chunks to Qdrant schema format"""
    
    def __init__(self):
        self.chunk_counter = 0
    
    def extract_ndc_from_products(self, json_data: Dict) -> Optional[str]:
        """Extract NDC code from manufactured products"""
        if 'manufactured_products' not in json_data:
            return None
        
        for product in json_data['manufactured_products']:
            if 'code' in product and 'code' in product['code']:
                ndc = product['code']['code']
                if ndc and '-' in ndc:  # Basic NDC format validation
                    return ndc
        return None
    
    def extract_generic_name(self, json_data: Dict) -> Optional[str]:
        """Extract generic medicine name from products"""
        if 'manufactured_products' not in json_data:
            return None
        
        for product in json_data['manufactured_products']:
            if 'genericMedicine' in product and product['genericMedicine']:
                # Take first generic name if multiple exist
                return product['genericMedicine'][0]
        return None
    
    def extract_dosage_form(self, json_data: Dict) -> Optional[str]:
        """Extract dosage form from products"""
        if 'manufactured_products' not in json_data:
            return None
        
        for product in json_data['manufactured_products']:
            if 'formCode' in product and 'displayName' in product['formCode']:
                return product['formCode']['displayName'].lower()
        return None
    
    def extract_route(self, json_data: Dict) -> Optional[str]:
        """Extract administration route from products"""
        if 'manufactured_products' not in json_data:
            return None
        
        for product in json_data['manufactured_products']:
            if 'consumedIn' in product and 'substanceAdministration' in product['consumedIn']:
                route_code = product['consumedIn']['substanceAdministration'].get('routeCode', {})
                if 'displayName' in route_code:
                    return route_code['displayName'].lower()
        return None
    
    def extract_document_metadata(self, json_data: Dict) -> Dict[str, Any]:
        """Extract document-level metadata"""
        metadata = {}
        
        if 'document' in json_data:
            doc = json_data['document']
            if 'code' in doc and 'displayName' in doc['code']:
                metadata['document_type'] = doc['code']['displayName']
            if 'versionNumber' in doc and 'value' in doc['versionNumber']:
                metadata['version_number'] = doc['versionNumber']['value']
            if 'setId' in doc and 'root' in doc['setId']:
                metadata['set_id'] = doc['setId']['root']
        
        return metadata
    
    def transform_chunk(self, chunk_data: Dict, original_json: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Transform a single text chunk to Qdrant schema format
        
        Args:
            chunk_data: Text chunk from extraction
            original_json: Original JSON data for additional fields
            
        Returns:
            Qdrant-compatible document
        """
        self.chunk_counter += 1
        
        # Generate unique chunk ID
        chunk_id = str(uuid.uuid4())
        
        # Extract additional fields from original JSON if available
        ndc = None
        generic_name = None
        dosage_form = None
        route = None
        doc_metadata = {}
        
        if original_json:
            ndc = self.extract_ndc_from_products(original_json)
            generic_name = self.extract_generic_name(original_json)
            dosage_form = self.extract_dosage_form(original_json)
            route = self.extract_route(original_json)
            doc_metadata = self.extract_document_metadata(original_json)
        
        # Build Qdrant document
        qdrant_doc = {
            "chunk_id": chunk_id,
            "medication_id": chunk_data.get('document_id'),
            "drug_name": chunk_data.get('product_name'),
            "generic_name": generic_name,
            "manufacturer": chunk_data.get('manufacturer'),
            "ndc": ndc,
            "dosage_form": dosage_form,
            "route": route,
            "section_type": chunk_data.get('section_type'),
            "section_title": chunk_data.get('section_title'),
            "loinc_code": chunk_data.get('loinc_code'),
            "chunk_index": chunk_data.get('chunk_index', 0),
            "total_chunks": chunk_data.get('total_chunks', 1),
            "text": chunk_data.get('text_content', ''),
            "word_count": len(chunk_data.get('text_content', '').split()),
            "metadata": {
                "source_file": chunk_data.get('source_file'),
                "effective_date": chunk_data.get('effective_date'),
                **doc_metadata
            }
        }
        
        return qdrant_doc
    
    def load_original_json(self, json_dir: str, document_id: str) -> Optional[Dict]:
        """Load original JSON file for additional metadata"""
        json_path = Path(json_dir)
        
        # Search for JSON file with matching document ID
        for json_file in json_path.glob("**/*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if (data.get('document', {}).get('id', {}).get('root') == document_id):
                        return data
            except Exception as e:
                logger.debug(f"Error reading {json_file}: {e}")
                continue
        
        logger.warning(f"Original JSON not found for document_id: {document_id}")
        return None
    
    def transform_chunks_file(self, chunks_file: str, original_json_dir: str, output_file: str) -> None:
        """
        Transform entire chunks file to Qdrant format
        
        Args:
            chunks_file: Path to extracted_text_chunks.jsonl
            original_json_dir: Directory containing original JSON files
            output_file: Output file for Qdrant documents
        """
        logger.info(f"Transforming chunks from {chunks_file}")
        logger.info(f"Using original JSON files from {original_json_dir}")
        
        # Cache for original JSON files to avoid repeated loading
        json_cache = {}
        
        qdrant_docs = []
        processed_count = 0
        
        with open(chunks_file, 'r', encoding='utf-8') as f:
            for line in f:
                chunk_data = json.loads(line.strip())
                document_id = chunk_data.get('document_id')
                
                # Load original JSON if not cached
                if document_id not in json_cache:
                    json_cache[document_id] = self.load_original_json(original_json_dir, document_id)
                
                # Transform chunk
                qdrant_doc = self.transform_chunk(chunk_data, json_cache[document_id])
                qdrant_docs.append(qdrant_doc)
                
                processed_count += 1
                if processed_count % 1000 == 0:
                    logger.info(f"Processed {processed_count} chunks")
        
        # Save transformed documents
        with open(output_file, 'w', encoding='utf-8') as f:
            for doc in qdrant_docs:
                f.write(json.dumps(doc) + '\n')
        
        logger.info(f"Transformation complete. Saved {len(qdrant_docs)} documents to {output_file}")
        
        # Generate summary statistics
        self._generate_transform_stats(qdrant_docs, output_file)
    
    def _generate_transform_stats(self, docs: List[Dict], output_file: str) -> None:
        """Generate transformation statistics"""
        stats = {
            'total_documents': len(docs),
            'documents_with_ndc': sum(1 for d in docs if d.get('ndc')),
            'documents_with_generic_name': sum(1 for d in docs if d.get('generic_name')),
            'documents_with_dosage_form': sum(1 for d in docs if d.get('dosage_form')),
            'documents_with_route': sum(1 for d in docs if d.get('route')),
            'section_type_distribution': {},
            'manufacturer_distribution': {},
            'avg_word_count': sum(d.get('word_count', 0) for d in docs) / len(docs) if docs else 0
        }
        
        # Count section types
        for doc in docs:
            section_type = doc.get('section_type', 'Unknown')
            stats['section_type_distribution'][section_type] = \
                stats['section_type_distribution'].get(section_type, 0) + 1
        
        # Count manufacturers (top 10)
        manufacturer_counts = {}
        for doc in docs:
            manufacturer = doc.get('manufacturer', 'Unknown')
            manufacturer_counts[manufacturer] = manufacturer_counts.get(manufacturer, 0) + 1
        
        stats['manufacturer_distribution'] = dict(
            sorted(manufacturer_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        )
        
        # Save stats
        stats_file = output_file.replace('.jsonl', '_stats.json')
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Statistics saved to {stats_file}")


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Transform text chunks to Qdrant schema format')
    parser.add_argument('chunks_file', help='Path to extracted_text_chunks.jsonl')
    parser.add_argument('json_dir', help='Directory containing original JSON files')
    parser.add_argument('output_file', help='Output file for Qdrant documents')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    transformer = QdrantTransformer()
    transformer.transform_chunks_file(args.chunks_file, args.json_dir, args.output_file)


if __name__ == "__main__":
    main()