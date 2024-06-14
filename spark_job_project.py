from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Buat Spark session
spark = SparkSession.builder.appName("OLTP_OLAP").getOrCreate()

# Muat data CSV ke dalam DataFrame
transactions_df = spark.read.csv("home/fauzipersib/finalproject/sk_data design produk.csv", header=True, inferSchema=True)
products_df = spark.read.csv("home/fauzipersib/finalproject/sk_data status produk.csv", header=True, inferSchema=True)

# Tampilkan beberapa data untuk memastikan data terload dengan benar
transactions_df.show(5)
products_df.show(5)

# Gabungkan transactions dengan products berdasarkan product_id
oltp_df = transactions_df.join(products_df, transactions_df.nama_customer == products_df.no.mc, "inner")

# Tampilkan skema OLTP
oltp_df.show(5)

# Agregasi total penjualan per kategori produk
olap_df = oltp_df.groupBy("category").agg(sum("amount").alias("total_sales"))

# Tampilkan skema OLAP
olap_df.show(5)

# Simpan OLTP DataFrame ke dalam file CSV
oltp_df.write.csv("home/fauzipersib/finalproject/output/oltp_data.csv", header=True)

# Simpan OLAP DataFrame ke dalam file CSV
olap_df.write.csv("home/fauzipersib/finalproject/output/olap_data_status.csv", header=True)