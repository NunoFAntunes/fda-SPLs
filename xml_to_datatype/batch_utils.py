#!/usr/bin/env python3
"""
Batch Processing Utilities

Helper utilities for managing batch XML processing operations.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Any, List
import psycopg2
from psycopg2.extras import RealDictCursor
from xml_to_database import DatabaseConfig
import logging


def count_xml_files(folder_path: Path) -> int:
    """Count total XML files in folder structure."""
    return len(list(folder_path.rglob("*.xml")))


def check_database_status(db_config: DatabaseConfig) -> Dict[str, Any]:
    """Check database connection and get status information."""
    status = {
        'connection': False,
        'documents_count': 0,
        'products_count': 0,
        'organizations_count': 0,
        'error': None
    }
    
    try:
        conn = psycopg2.connect(db_config.get_connection_string())
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        status['connection'] = True
        
        # Get document count
        cursor.execute("SELECT COUNT(*) as count FROM documents")
        result = cursor.fetchone()
        status['documents_count'] = result['count'] if result else 0
        
        # Get product count
        cursor.execute("SELECT COUNT(*) as count FROM products")
        result = cursor.fetchone()
        status['products_count'] = result['count'] if result else 0
        
        # Get organization count
        cursor.execute("SELECT COUNT(*) as count FROM organizations")
        result = cursor.fetchone()
        status['organizations_count'] = result['count'] if result else 0
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        status['error'] = str(e)
    
    return status


def get_processing_estimates(xml_count: int, workers: int = 4) -> Dict[str, Any]:
    """Estimate processing time and resource requirements."""
    # Based on empirical testing - adjust these rates as needed
    base_rate = 1.2  # files per second with 4 workers
    worker_scaling = 0.8  # efficiency decrease per additional worker
    
    effective_rate = base_rate * (1 + (workers - 4) * worker_scaling)
    estimated_seconds = xml_count / effective_rate if effective_rate > 0 else xml_count
    
    return {
        'xml_files': xml_count,
        'workers': workers,
        'estimated_time_seconds': estimated_seconds,
        'estimated_time_minutes': estimated_seconds / 60,
        'estimated_time_hours': estimated_seconds / 3600,
        'processing_rate': effective_rate,
        'memory_estimate_mb': max(100, xml_count * 0.01),  # Rough estimate
    }


def analyze_processing_report(report_file: Path) -> Dict[str, Any]:
    """Analyze a JSON processing report and provide insights."""
    try:
        with open(report_file, 'r', encoding='utf-8') as f:
            report = json.load(f)
    except Exception as e:
        return {'error': f"Failed to load report: {e}"}
    
    summary = report.get('summary', {})
    errors = report.get('errors', [])
    results = report.get('results', [])
    
    # Error analysis
    error_types = {}
    for error in errors:
        error_msg = error.get('error', 'Unknown error')
        # Categorize errors
        if 'XML parsing' in error_msg or 'not well-formed' in error_msg:
            error_type = 'XML Parsing Errors'
        elif 'database' in error_msg.lower() or 'connection' in error_msg.lower():
            error_type = 'Database Errors'
        elif 'timeout' in error_msg.lower():
            error_type = 'Timeout Errors'
        else:
            error_type = 'Other Errors'
        
        error_types[error_type] = error_types.get(error_type, 0) + 1
    
    # Performance analysis
    successful_results = [r for r in results if r.get('success', False)]
    processing_times = [r.get('processing_time', 0) for r in successful_results if r.get('processing_time')]
    
    avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
    slowest_files = sorted(
        [(r.get('file', ''), r.get('processing_time', 0)) for r in successful_results],
        key=lambda x: x[1], reverse=True
    )[:5]
    
    return {
        'summary': summary,
        'error_analysis': error_types,
        'performance': {
            'average_processing_time': avg_processing_time,
            'slowest_files': slowest_files,
            'total_successful': len(successful_results)
        },
        'recommendations': generate_recommendations(summary, error_types, avg_processing_time)
    }


def generate_recommendations(summary: Dict, error_types: Dict, avg_time: float) -> List[str]:
    """Generate optimization recommendations based on processing results."""
    recommendations = []
    
    success_rate = summary.get('success_rate', 0)
    processing_rate = summary.get('processing_rate', 0)
    
    # Success rate recommendations
    if success_rate < 90:
        recommendations.append("Low success rate - check XML file quality and database connectivity")
    
    if 'XML Parsing Errors' in error_types and error_types['XML Parsing Errors'] > 5:
        recommendations.append("Many XML parsing errors - verify file encoding and structure")
    
    if 'Database Errors' in error_types:
        recommendations.append("Database errors detected - check connection stability and timeouts")
    
    # Performance recommendations
    if processing_rate < 0.5:
        recommendations.append("Low processing rate - consider reducing worker count or checking database performance")
    elif processing_rate > 2.0:
        recommendations.append("Good processing rate - you might be able to increase worker count")
    
    if avg_time > 2.0:
        recommendations.append("High average processing time - check for complex XML files or database bottlenecks")
    
    return recommendations


def cleanup_failed_imports(db_config: DatabaseConfig, document_ids: List[str] = None) -> Dict[str, Any]:
    """Clean up partial imports from failed processing."""
    result = {
        'deleted_documents': 0,
        'deleted_products': 0,
        'error': None
    }
    
    try:
        conn = psycopg2.connect(db_config.get_connection_string())
        cursor = conn.cursor()
        
        if document_ids:
            # Clean up specific documents
            placeholders = ','.join(['%s'] * len(document_ids))
            
            cursor.execute(f"""
                DELETE FROM documents WHERE document_id_root IN ({placeholders})
            """, document_ids)
            
            result['deleted_documents'] = cursor.rowcount
        else:
            # This would be dangerous - require explicit document IDs
            result['error'] = "Document IDs must be specified for cleanup"
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        result['error'] = str(e)
    
    return result


def main():
    """Command line interface for batch utilities."""
    parser = argparse.ArgumentParser(description='Batch processing utilities')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Count command
    count_parser = subparsers.add_parser('count', help='Count XML files in folder')
    count_parser.add_argument('folder', help='Folder to scan')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Check database status')
    status_parser.add_argument('--host', default='localhost')
    status_parser.add_argument('--port', type=int, default=5432)
    status_parser.add_argument('--database', default='fda_spls')
    status_parser.add_argument('--user', default='postgres')
    status_parser.add_argument('--password', default='postgres')
    
    # Estimate command
    estimate_parser = subparsers.add_parser('estimate', help='Estimate processing time')
    estimate_parser.add_argument('folder', help='Folder to analyze')
    estimate_parser.add_argument('--workers', type=int, default=4, help='Number of workers')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze processing report')
    analyze_parser.add_argument('report', help='JSON report file')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        if args.command == 'count':
            folder = Path(args.folder)
            if not folder.exists():
                logger.error(f"Folder not found: {args.folder}")
                sys.exit(1)
            
            count = count_xml_files(folder)
            logger.info(f"XML files found: {count:,}")
        
        elif args.command == 'status':
            db_config = DatabaseConfig(
                host=args.host,
                port=args.port,
                database=args.database,
                user=args.user,
                password=args.password
            )
            
            status = check_database_status(db_config)
            
            if status['connection']:
                logger.info("✓ Database connection successful")
                logger.info(f"Documents in database: {status['documents_count']:,}")
                logger.info(f"Products in database: {status['products_count']:,}")
                logger.info(f"Organizations in database: {status['organizations_count']:,}")
            else:
                logger.error(f"✗ Database connection failed: {status['error']}")
                sys.exit(1)
        
        elif args.command == 'estimate':
            folder = Path(args.folder)
            if not folder.exists():
                logger.error(f"Folder not found: {args.folder}")
                sys.exit(1)
            
            xml_count = count_xml_files(folder)
            estimates = get_processing_estimates(xml_count, args.workers)
            
            logger.info(f"XML files to process: {estimates['xml_files']:,}")
            logger.info(f"Workers: {estimates['workers']}")
            logger.info(f"Estimated processing rate: {estimates['processing_rate']:.2f} files/second")
            logger.info(f"Estimated time: {estimates['estimated_time_minutes']:.1f} minutes ({estimates['estimated_time_hours']:.1f} hours)")
            logger.info(f"Estimated memory usage: {estimates['memory_estimate_mb']:.0f} MB")
        
        elif args.command == 'analyze':
            report_file = Path(args.report)
            if not report_file.exists():
                logger.error(f"Report file not found: {args.report}")
                sys.exit(1)
            
            analysis = analyze_processing_report(report_file)
            
            if 'error' in analysis:
                logger.error(f"Analysis failed: {analysis['error']}")
                sys.exit(1)
            
            # Print analysis results
            summary = analysis['summary']
            logger.info("=== PROCESSING SUMMARY ===")
            logger.info(f"Success rate: {summary.get('success_rate', 0):.1f}%")
            logger.info(f"Processing rate: {summary.get('processing_rate', 0):.2f} files/second")
            logger.info(f"Total time: {summary.get('elapsed_time_seconds', 0):.1f} seconds")
            
            logger.info("\n=== ERROR ANALYSIS ===")
            for error_type, count in analysis['error_analysis'].items():
                logger.info(f"{error_type}: {count}")
            
            logger.info("\n=== PERFORMANCE ===")
            perf = analysis['performance']
            logger.info(f"Average processing time: {perf['average_processing_time']:.3f} seconds")
            logger.info("Slowest files:")
            for file_path, time_taken in perf['slowest_files']:
                logger.info(f"  {Path(file_path).name}: {time_taken:.3f}s")
            
            logger.info("\n=== RECOMMENDATIONS ===")
            for rec in analysis['recommendations']:
                logger.info(f"• {rec}")
    
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()