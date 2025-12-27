from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
from extract.scimagojr.scimagojr_extractor import ScimagoJRExtractor

default_args = {
    'owner': 'impactu',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_extraction_by_year(year, **kwargs):
    # Use MongoHook to get the connection
    hook = MongoHook(mongo_conn_id='mongodb_default')
    client = hook.get_conn()
    db_name = hook.connection.schema or 'impactu'
    
    extractor = ScimagoJRExtractor(None, db_name, client=client)
    # The extractor now uses the airflow task logger automatically
    try:
        data = extractor.fetch_year(year)
        if not data:
            print(f"No data found for year {year}")
            return

        print(f"Processing {len(data)} records for year {year}...")
        
        from pymongo import UpdateOne
        import pandas as pd
        operations = []
        for record in data:
            # Sanitize keys (replace dots) and handle NaN
            sanitized_record = {
                k.replace('.', '_'): (None if pd.isna(v) else v) 
                for k, v in record.items()
            }
            
            # Use Sourceid and year as unique identifier
            source_id = sanitized_record.get('Sourceid')
            if source_id:
                operations.append(
                    UpdateOne(
                        {"Sourceid": source_id, "year": year},
                        {"$set": sanitized_record},
                        upsert=True
                    )
                )
        
        if operations:
            result = extractor.collection.bulk_write(operations, ordered=False)
            print(
                f"Year {year}: {result.upserted_count} new, "
                f"{result.modified_count} changed, "
                f"{result.matched_count} unchanged."
            )
    finally:
        extractor.close()

with DAG(
    'extract_scimagojr',
    default_args=default_args,
    description='Extract data from ScimagoJR and load into MongoDB',
    schedule='@yearly',
    catchup=False,
    tags=['extract', 'scimagojr'],
) as dag:

    years = list(range(1999, datetime.now().year + 1))

    extract_task = PythonOperator.partial(
        task_id='extract_and_load_scimagojr',
        python_callable=run_extraction_by_year,
    ).expand(op_kwargs=[{'year': year} for year in years])
