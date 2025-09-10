#!/usr/bin/env python3
"""
Qdrant Vector Database Setup and Management
Set up collections, create embeddings, and ingest documents
"""

import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
import uuid

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct
    from sentence_transformers import SentenceTransformer
    import openai
    import numpy as np
    from tqdm import tqdm
    import os
except ImportError as e:
    print(f"Missing dependencies. Install with: pip install -r requirements.txt")
    print(f"Error: {e}")
    exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QdrantManager:
    """Manage Qdrant vector database operations"""
    
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 6333,
                 embedding_model: str = "text-embedding-3-small",
                 openai_api_key: Optional[str] = None):
        """
        Initialize Qdrant manager
        
        Args:
            host: Qdrant server host
            port: Qdrant server port  
            embedding_model: Embedding model name (OpenAI or SentenceTransformer)
            openai_api_key: OpenAI API key (optional, will use OPENAI_API_KEY env var if not provided)
        """
        self.client = QdrantClient(host=host, port=port)
        self.embedding_model_name = embedding_model
        self.embedding_model = None
        self.vector_size = None
        self.is_openai_model = embedding_model.startswith('text-embedding')
        
        # Initialize OpenAI client if using OpenAI model
        if self.is_openai_model:
            # Use provided API key or fallback to environment variable
            api_key = openai_api_key or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key required. Provide via --api-key argument or OPENAI_API_KEY environment variable")
            self.openai_client = openai.OpenAI(api_key=api_key)
            
            # Log key source (without revealing the key)
            key_source = "command line argument" if openai_api_key else "environment variable"
            logger.info(f"Using OpenAI embedding model: {embedding_model} (API key from {key_source})")
        
        # Initialize embedding model
        self._load_embedding_model()
    
    def _load_embedding_model(self):
        """Load the embedding model (OpenAI or SentenceTransformer)"""
        if self.is_openai_model:
            # For OpenAI models, get vector size from API docs
            model_dimensions = {
                'text-embedding-3-small': 1536,
                'text-embedding-3-large': 3072,
                'text-embedding-ada-002': 1536
            }
            self.vector_size = model_dimensions.get(self.embedding_model_name, 1536)
            logger.info(f"OpenAI embedding model configured. Vector size: {self.vector_size}")
        else:
            logger.info(f"Loading SentenceTransformer model: {self.embedding_model_name}")
            self.embedding_model = SentenceTransformer(self.embedding_model_name)
            
            # Get vector dimension by encoding a test string
            test_embedding = self.embedding_model.encode("test")
            self.vector_size = len(test_embedding)
            logger.info(f"SentenceTransformer model loaded. Vector size: {self.vector_size}")
    
    def create_collection(self, collection_name: str, recreate: bool = False) -> bool:
        """
        Create a Qdrant collection for FDA SPL documents
        
        Args:
            collection_name: Name of the collection
            recreate: Whether to recreate if exists
            
        Returns:
            True if successful
        """
        try:
            # Check if collection exists
            collections = self.client.get_collections()
            collection_exists = any(col.name == collection_name for col in collections.collections)
            
            if collection_exists:
                if recreate:
                    logger.info(f"Deleting existing collection: {collection_name}")
                    self.client.delete_collection(collection_name)
                else:
                    logger.info(f"Collection {collection_name} already exists")
                    return True
            
            # Create collection
            logger.info(f"Creating collection: {collection_name}")
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=self.vector_size,
                    distance=Distance.COSINE
                )
            )
            
            logger.info(f"Collection {collection_name} created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            return False
    
    def create_embeddings(self, texts: List[str], batch_size: int = 100) -> np.ndarray:
        """
        Create embeddings for a list of texts using OpenAI or SentenceTransformer
        
        Args:
            texts: List of texts to embed
            batch_size: Batch size for processing (larger for OpenAI API efficiency)
            
        Returns:
            Array of embeddings
        """
        logger.info(f"Creating embeddings for {len(texts)} texts")
        
        if self.is_openai_model:
            return self._create_openai_embeddings(texts, batch_size)
        else:
            return self._create_sentence_transformer_embeddings(texts, batch_size)
    
    def _create_openai_embeddings(self, texts: List[str], batch_size: int) -> np.ndarray:
        """Create embeddings using OpenAI API"""
        embeddings = []
        total_tokens = 0
        
        for i in tqdm(range(0, len(texts), batch_size), desc="Creating OpenAI embeddings"):
            batch = texts[i:i + batch_size]
            
            try:
                response = self.openai_client.embeddings.create(
                    input=batch,
                    model=self.embedding_model_name
                )
                
                batch_embeddings = [data.embedding for data in response.data]
                embeddings.extend(batch_embeddings)
                total_tokens += response.usage.total_tokens
                
            except Exception as e:
                logger.error(f"Error creating embeddings for batch {i//batch_size + 1}: {e}")
                # Create zero vectors as fallback
                fallback_embeddings = [[0.0] * self.vector_size for _ in batch]
                embeddings.extend(fallback_embeddings)
        
        logger.info(f"OpenAI embeddings created. Total tokens used: {total_tokens}")
        estimated_cost = total_tokens * 0.00002  # $0.02 per 1M tokens for text-embedding-3-small
        logger.info(f"Estimated cost: ${estimated_cost:.4f}")
        
        return np.array(embeddings)
    
    def _create_sentence_transformer_embeddings(self, texts: List[str], batch_size: int) -> np.ndarray:
        """Create embeddings using SentenceTransformers"""
        embeddings = []
        for i in tqdm(range(0, len(texts), batch_size), desc="Creating embeddings"):
            batch = texts[i:i + batch_size]
            batch_embeddings = self.embedding_model.encode(batch, convert_to_numpy=True)
            embeddings.extend(batch_embeddings)
        
        return np.array(embeddings)
    
    def prepare_document_for_ingestion(self, doc: Dict[str, Any], embedding: np.ndarray) -> PointStruct:
        """
        Prepare a document for Qdrant ingestion
        
        Args:
            doc: Document dictionary
            embedding: Document embedding vector
            
        Returns:
            PointStruct for Qdrant
        """
        # Use chunk_id as point ID, or generate if missing
        point_id = doc.get('chunk_id', str(uuid.uuid4()))
        
        # Prepare payload (all fields except embedding)
        payload = {key: value for key, value in doc.items() if key != 'embedding'}
        
        return PointStruct(
            id=point_id,
            vector=embedding.tolist(),
            payload=payload
        )
    
    def ingest_documents(self, 
                        collection_name: str,
                        documents_file: str, 
                        batch_size: int = 100) -> bool:
        """
        Ingest documents into Qdrant collection
        
        Args:
            collection_name: Target collection name
            documents_file: Path to JSONL file with documents
            batch_size: Batch size for ingestion
            
        Returns:
            True if successful
        """
        try:
            logger.info(f"Starting document ingestion from {documents_file}")
            
            # Load documents
            documents = []
            with open(documents_file, 'r', encoding='utf-8') as f:
                for line in f:
                    doc = json.loads(line.strip())
                    documents.append(doc)
            
            logger.info(f"Loaded {len(documents)} documents")
            
            # Create embeddings for all texts (handle both 'text' and 'text_content' fields)
            texts = []
            for doc in documents:
                if 'text' in doc:
                    texts.append(doc['text'])
                elif 'text_content' in doc:
                    texts.append(doc['text_content'])
                else:
                    logger.error(f"No text field found in document: {doc.keys()}")
                    texts.append("")  # fallback empty string
            embeddings = self.create_embeddings(texts)
            
            # Prepare points for ingestion
            points = []
            for doc, embedding in zip(documents, embeddings):
                point = self.prepare_document_for_ingestion(doc, embedding)
                points.append(point)
            
            # Ingest in batches
            total_batches = (len(points) + batch_size - 1) // batch_size
            
            for i in tqdm(range(0, len(points), batch_size), 
                         desc="Ingesting documents", 
                         total=total_batches):
                batch = points[i:i + batch_size]
                
                self.client.upsert(
                    collection_name=collection_name,
                    points=batch
                )
            
            logger.info(f"Successfully ingested {len(points)} documents into {collection_name}")
            
            # Get collection info
            info = self.client.get_collection(collection_name)
            logger.info(f"Collection info: {info}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting documents: {e}")
            return False
    
    def search_similar(self, 
                      collection_name: str,
                      query_text: str, 
                      limit: int = 5,
                      filters: Optional[Dict] = None) -> List[Dict]:
        """
        Search for similar documents
        
        Args:
            collection_name: Collection to search
            query_text: Query text
            limit: Number of results to return
            filters: Optional filters to apply
            
        Returns:
            List of search results
        """
        try:
            # Create embedding for query
            if self.is_openai_model:
                response = self.openai_client.embeddings.create(
                    input=[query_text],
                    model=self.embedding_model_name
                )
                query_embedding = np.array(response.data[0].embedding)
            else:
                query_embedding = self.embedding_model.encode(query_text)
            
            # Perform search
            results = self.client.search(
                collection_name=collection_name,
                query_vector=query_embedding.tolist(),
                limit=limit,
                query_filter=filters
            )
            
            # Format results
            formatted_results = []
            for result in results:
                formatted_results.append({
                    'score': result.score,
                    'document': result.payload,
                    'text': result.payload.get('text', '')[:200] + '...'
                })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Error searching: {e}")
            return []
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get collection statistics"""
        try:
            info = self.client.get_collection(collection_name)
            return {
                'status': info.status,
                'vectors_count': info.vectors_count,
                'indexed_vectors_count': info.indexed_vectors_count,
                'points_count': info.points_count,
                'config': {
                    'vector_size': info.config.params.vectors.size,
                    'distance': info.config.params.vectors.distance
                }
            }
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return {}


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Qdrant vector database management')
    parser.add_argument('action', choices=['create', 'ingest', 'search', 'stats'], 
                       help='Action to perform')
    parser.add_argument('--collection', default='fda_spl_chunks', 
                       help='Collection name')
    parser.add_argument('--documents', help='Path to documents JSONL file')
    parser.add_argument('--query', help='Search query text')
    parser.add_argument('--limit', type=int, default=5, help='Search result limit')
    parser.add_argument('--host', default='localhost', help='Qdrant host')
    parser.add_argument('--port', type=int, default=6333, help='Qdrant port')
    parser.add_argument('--model', default='text-embedding-3-small', 
                       help='Embedding model name. Options: text-embedding-3-small (default), text-embedding-3-large, pritamdeka/S-PubMedBert-MS-MARCO, BAAI/bge-large-en-v1.5')
    parser.add_argument('--api-key', help='OpenAI API key (optional, will use OPENAI_API_KEY env var if not provided)')
    parser.add_argument('--recreate', action='store_true', 
                       help='Recreate collection if exists')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize manager
    manager = QdrantManager(
        host=args.host, 
        port=args.port, 
        embedding_model=args.model,
        openai_api_key=getattr(args, 'api_key', None)
    )
    
    if args.action == 'create':
        success = manager.create_collection(args.collection, recreate=args.recreate)
        exit(0 if success else 1)
    
    elif args.action == 'ingest':
        if not args.documents:
            print("Error: --documents required for ingest action")
            exit(1)
        success = manager.ingest_documents(args.collection, args.documents)
        exit(0 if success else 1)
    
    elif args.action == 'search':
        if not args.query:
            print("Error: --query required for search action")
            exit(1)
        results = manager.search_similar(args.collection, args.query, limit=args.limit)
        for i, result in enumerate(results, 1):
            print(f"\n--- Result {i} (Score: {result['score']:.4f}) ---")
            print(f"Drug: {result['document'].get('drug_name', 'N/A')}")
            print(f"Section: {result['document'].get('section_type', 'N/A')}")
            print(f"Text: {result['text']}")
    
    elif args.action == 'stats':
        stats = manager.get_collection_stats(args.collection)
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()