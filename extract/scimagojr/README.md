# ScimagoJR Extractor

This module handles the extraction of journal ranking data from ScimagoJR.

## Features
*   **Checkpoints**: Uses a MongoDB collection (`etl_checkpoints`) to track which years have already been processed.
*   **Idempotency**: If a year is re-executed, the system updates existing records or inserts new ones (upsert), avoiding duplicates.
*   **Normalization**: Converts ScimagoJR's CSV/XLS format into MongoDB-ready JSON documents.

## Extraction Strategy

The extractor follows a robust multi-stage process to ensure data integrity and performance:

1.  **Persistent Caching**: Data is first downloaded and stored in a local cache (`/opt/airflow/cache/scimagojr`). If a file exists, it is reused unless `force_redownload` is triggered.
2.  **Database Indexing**: Before processing, unique compound indexes are ensured on `Sourceid` and `year` to prevent duplicates at the storage level.
3.  **Differential Synchronization (Upsert)**:
    *   The system compares incoming records with existing ones in MongoDB.
    *   **Updates**: If a record exists but its content has changed, it is updated.
    *   **Inserts**: Records not present in the database are inserted in bulk.
4.  **Automated Sanitization**:
    *   **Integrity Check**: After processing a year, the system verifies that the number of records in the database matches the source file count.
    *   **Duplicate Cleanup**: A MongoDB aggregation pipeline runs to identify and remove any redundant records (e.g., those caused by type mismatches like `int32` vs `int64`), ensuring a 1:1 mapping between source and destination.

## Usage
The extractor can be called from an Airflow DAG or as a standalone script.

```python
from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor
extractor = ScimagoJRExtractor(mongodb_uri, db_name)
extractor.run(1999, 2023)
```
