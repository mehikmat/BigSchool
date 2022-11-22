import pyarrow.parquet as pq

df = pq.ParquetFile("part-r-00000.snappy.parquet")

md = df.metadata.row_group(0).column(5)

print(md)