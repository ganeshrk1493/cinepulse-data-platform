import gzip
import io
import urllib.request
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage

PROJECT_ID = "cinepulse-analytics"
BUCKET_NAME = f"cinepulse-lake-{PROJECT_ID}"

IMDB_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_RATINGS_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"

def get_gcs_bucket():
    client = storage.Client(project=PROJECT_ID)
    return client.bucket(BUCKET_NAME)

def download_and_stage_imdb_basics(bucket):
    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching IMDb title.basics...")
    req = urllib.request.Request(
        IMDB_BASICS_URL, 
        headers={'User-Agent': 'Mozilla/5.0'}
    )
    
    with urllib.request.urlopen(req) as response:
        with gzip.GzipFile(fileobj=response) as uncompressed:
            # Read in chunks to remain memory efficient
            chunks = []
            for chunk in pd.read_csv(
                uncompressed, 
                sep='\t', 
                na_values='\\N', 
                low_memory=False, 
                chunksize=100000,
                dtype={
                    'tconst': str,
                    'titleType': str,
                    'primaryTitle': str,
                    'originalTitle': str,
                    'isAdult': str,
                    'startYear': str,
                    'endYear': str,
                    'runtimeMinutes': str,
                    'genres': str
                }
            ):
                # Filter for feature films released 2015 onwards
                filtered = chunk[
                    (chunk['titleType'].isin(['movie', 'tvMovie'])) & 
                    (chunk['startYear'] >= '2015')
                ]
                chunks.append(filtered)
            
            df_basics = pd.concat(chunks, ignore_index=True)
            print(f"Filtered to {len(df_basics):,} relevant movies.")

    # Upload to GCS as Parquet
    parquet_buffer = io.BytesIO()
    df_basics.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    blob_path = "raw/imdb/title_basics/title_basics.parquet"
    blob = bucket.blob(blob_path)
    blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
    print(f" Uploaded title_basics to gs://{BUCKET_NAME}/{blob_path}")

def download_and_stage_imdb_ratings(bucket):
    print(f"[{datetime.now(timezone.utc).isoformat()}] Fetching IMDb title.ratings...")
    req = urllib.request.Request(
        IMDB_RATINGS_URL, 
        headers={'User-Agent': 'Mozilla/5.0'}
    )
    
    with urllib.request.urlopen(req) as response:
        with gzip.GzipFile(fileobj=response) as uncompressed:
            df_ratings = pd.read_csv(
                uncompressed, 
                sep='\t', 
                na_values='\\N',
                dtype={'tconst': str, 'averageRating': float, 'numVotes': int}
            )
            print(f"Loaded {len(df_ratings):,} rating records.")

    parquet_buffer = io.BytesIO()
    df_ratings.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)

    blob_path = "raw/imdb/title_ratings/title_ratings.parquet"
    blob = bucket.blob(blob_path)
    blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
    print(f" Uploaded title_ratings to gs://{BUCKET_NAME}/{blob_path}")

def run_batch_ingest():
    bucket = get_gcs_bucket()
    download_and_stage_imdb_basics(bucket)
    download_and_stage_imdb_ratings(bucket)
    print(f"\n[{datetime.now(timezone.utc).isoformat()}] Batch ingestion to GCS complete!")

if __name__ == "__main__":
    run_batch_ingest()