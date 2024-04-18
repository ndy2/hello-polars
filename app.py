import tempfile

import polars as pl
import urllib3
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from minio import Minio

# temp_dir = tempfile.gettempdir()
temp_dir = "/Users/deukyun/github/hello-minio"
minio = Minio(
    "minio.minio:31183",
    access_key="minio",
    secret_key="minio123",
    secure=True,
    http_client=urllib3.PoolManager(
        cert_reqs="CERT_REQUIRED",
        ca_certs="~/.mc/certs/CAs/ca.crt"
    )
)

buckets = minio.list_buckets()
for bucket in buckets:
    print(bucket.name)
print("===============")

parquet_part_object = minio.list_objects(
    bucket_name="mybucket",
    prefix="myparquet",
    recursive=True
)

for part in parquet_part_object:
    object_name = part.object_name
    file_path = f"{temp_dir}/{part.object_name.split('/')[-1]}"
    part_object = minio.fget_object(
        bucket_name="mybucket",
        object_name=object_name,
        file_path=file_path
    )
    print("fget_object : file path : ", file_path)

print("===============")
df = pl.read_parquet(f"{temp_dir}/*.parquet")
print(df)
print(df.head())
print(df.schema)

df.write_csv(f"{temp_dir}/data.csv")

# Cassandra에 연결
auth_provider = PlainTextAuthProvider(username='cassandra', password='MeUhRHK0hv')
cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
session = cluster.connect('some')

execute = session.execute("COPY some.members")
print(execute)

for e in df:
    print(e.name)

for row in df.rows():
    print(row)
