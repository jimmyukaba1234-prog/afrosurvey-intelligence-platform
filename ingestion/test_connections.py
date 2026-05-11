import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="airflow",
    password="airflow",
    database="airflow"
)

print("PostgreSQL connection successful!")

conn.close()


from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

buckets = client.list_buckets()

for bucket in buckets:
    print(bucket.name)