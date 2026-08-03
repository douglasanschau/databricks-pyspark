vendedores = spark.read.parquet(
    "/Volumes/workspace/default/bronze/vendedores"
);

vendedores.write \
      .mode("overwrite") \
      .parquet("/Volumes/workspace/default/silver/vendedores");