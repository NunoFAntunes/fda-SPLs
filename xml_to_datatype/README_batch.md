# Batch XML to Database Processor

This script processes entire folders of FDA SPL XML files, converting them to the PostgreSQL database in parallel for high-performance bulk imports.

## Features

### 🚀 **High Performance**
- **Parallel Processing**: Multi-threaded processing with configurable worker count
- **Duplicate Detection**: Automatically skips already-processed documents
- **Progress Tracking**: Real-time progress updates and statistics
- **Memory Efficient**: Processes files individually without loading entire dataset

### 🛡️ **Robust Error Handling**
- **Continue on Error**: Keeps processing remaining files if one fails
- **Transaction Safety**: Each file processed in its own database transaction
- **Comprehensive Logging**: Detailed logs saved to file and console
- **Error Reporting**: Complete error summary with file-level details

### 📊 **Monitoring & Reporting**
- **Real-time Statistics**: Success rates, processing speed, file counts
- **JSON Reports**: Detailed processing reports with error details
- **Duplicate Tracking**: Lists all skipped duplicate files
- **Performance Metrics**: Processing time and throughput analysis

## Prerequisites

1. **Database Setup**:
   ```bash
   cd deploy
   docker-compose up -d postgres
   ```

2. **Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage
```bash
# Process all XML files in a folder
python batch_xml_to_database.py /path/to/xml/folder

# Process with custom database connection
python batch_xml_to_database.py /path/to/xml/folder \
  --host localhost \
  --port 5432 \
  --database fda_spls \
  --user postgres \
  --password postgres
```

### Performance Options
```bash
# Use 8 parallel workers (default: 4)
python batch_xml_to_database.py /path/to/xml/folder --workers 8

# Sequential processing (no parallelism)
python batch_xml_to_database.py /path/to/xml/folder --workers 1
```

### Error Handling Options
```bash
# Stop on first error (default: continue)
python batch_xml_to_database.py /path/to/xml/folder --stop-on-error

# Process duplicates instead of skipping
python batch_xml_to_database.py /path/to/xml/folder --no-skip-duplicates
```

### Reporting Options
```bash
# Enable verbose logging
python batch_xml_to_database.py /path/to/xml/folder --verbose

# Save detailed JSON report
python batch_xml_to_database.py /path/to/xml/folder --report processing_report.json
```

## Real-World Examples

### Process FDA SPL Data Extract
```bash
# Process entire OTC drug database
python batch_xml_to_database.py \
  "C:\code\fda-SPLs\data\extracted\dm_spl_release_human_otc_part1\otc" \
  --workers 6 \
  --verbose \
  --report otc_import_report.json

# Process prescription drug database
python batch_xml_to_database.py \
  "C:\code\fda-SPLs\data\extracted\dm_spl_release_human_rx_part1\prescription" \
  --workers 8 \
  --report rx_import_report.json
```

### Incremental Updates
```bash
# Process only new files (skip duplicates automatically)
python batch_xml_to_database.py /path/to/new/xml/files
```

## Output Examples

### Console Output
```bash
$ python batch_xml_to_database.py data/otc --workers 4 --verbose

2024-01-15 10:30:00 - INFO - Testing database connection...
2024-01-15 10:30:01 - INFO - Database connection successful
2024-01-15 10:30:01 - INFO - Starting batch processing of folder: data/otc
2024-01-15 10:30:02 - INFO - Scanning for XML files in: data/otc
2024-01-15 10:30:03 - INFO - Found 1,247 XML files
2024-01-15 10:30:03 - INFO - Processing files in parallel with 4 workers
2024-01-15 10:30:13 - INFO - Progress: 10/1247 files, 8 successful, 1 failed, 1 skipped
2024-01-15 10:30:23 - INFO - Progress: 20/1247 files, 17 successful, 2 failed, 1 skipped
...
2024-01-15 10:45:30 - INFO - Progress: 1247/1247 files, 1201 successful, 23 failed, 23 skipped

============================================================
BATCH PROCESSING COMPLETE
============================================================
Total files found: 1,247
Files processed: 1,247
Successfully imported: 1,201
Failed: 23
Skipped (duplicates): 23
Success rate: 96.3%
Processing time: 927.4 seconds
Processing rate: 1.34 files/second

All files processed successfully!
```

### Error Summary
```bash
============================================================
PROCESSING ERRORS:
============================================================
FILE: /path/to/corrupted_file.xml
ERROR: XML parsing failed: not well-formed (invalid token)
----------------------------------------
FILE: /path/to/incomplete_file.xml
ERROR: Required element 'id' not found in document
----------------------------------------
... and 21 more errors (see report for full list)
```

### JSON Report Structure
```json
{
  "summary": {
    "total_files": 1247,
    "processed_files": 1247,
    "successful_files": 1201,
    "failed_files": 23,
    "skipped_files": 23,
    "duplicates_count": 23,
    "elapsed_time_seconds": 927.4,
    "processing_rate": 1.34,
    "success_rate": 96.3
  },
  "errors": [
    {
      "file": "/path/to/failed_file.xml",
      "error": "Database connection timeout",
      "timestamp": "2024-01-15T10:35:22"
    }
  ],
  "duplicates": [
    "/path/to/duplicate_file.xml"
  ],
  "results": [
    {
      "file": "/path/to/file.xml",
      "success": true,
      "document_id": "f119f7a7-c1a8-44a6-a678-0e13c46294fd",
      "products_count": 1,
      "processing_time": 0.234
    }
  ]
}
```

## Performance Guidelines

### Optimal Worker Count
- **CPU-bound**: Start with `workers = CPU cores`
- **I/O-bound**: Can use `workers = CPU cores * 2`
- **Database-limited**: Reduce workers if database becomes bottleneck
- **Memory-limited**: Reduce workers if running out of memory

### Recommended Settings by Dataset Size

| Dataset Size | Workers | Expected Time | Memory Usage |
|-------------|---------|---------------|--------------|
| < 100 files | 2-4 | < 5 minutes | Low |
| 100-1,000 files | 4-6 | 5-30 minutes | Medium |
| 1,000-10,000 files | 6-8 | 30-300 minutes | High |
| > 10,000 files | 8-12 | > 5 hours | Very High |

## Troubleshooting

### Common Issues

**Database Connection Errors**:
```bash
# Check if container is running
docker ps | grep postgres

# Start database if stopped
cd deploy && docker-compose up -d postgres
```

**Memory Issues with Large Datasets**:
```bash
# Reduce worker count
python batch_xml_to_database.py folder --workers 2

# Process in smaller batches
python batch_xml_to_database.py folder/part1 --workers 4
python batch_xml_to_database.py folder/part2 --workers 4
```

**XML Parsing Errors**:
- Check XML file encoding (should be UTF-8)
- Verify XML files are not truncated
- Look for special characters in file paths

**Duplicate Processing**:
```bash
# Skip duplicates (default behavior)
python batch_xml_to_database.py folder

# Force reprocessing of duplicates
python batch_xml_to_database.py folder --no-skip-duplicates
```

### Log Files
- **Console logs**: Real-time progress and errors
- **batch_processing.log**: Complete processing log saved to file
- **JSON report**: Structured data for analysis and reporting

## Database Monitoring

While processing, you can monitor database activity:

```sql
-- Check import progress
SELECT COUNT(*) as total_documents FROM documents;
SELECT COUNT(*) as total_products FROM products;

-- Monitor active connections
SELECT count(*) as active_connections 
FROM pg_stat_activity 
WHERE state = 'active';

-- Check recent imports
SELECT document_id_root, created_at 
FROM documents 
ORDER BY created_at DESC 
LIMIT 10;
```

## Integration with Existing Workflows

The batch processor is designed to work seamlessly with:
- **Cron jobs**: For scheduled data updates
- **CI/CD pipelines**: For automated testing and deployment
- **Data validation**: Can be chained with validation scripts
- **Monitoring systems**: JSON reports integrate with monitoring tools