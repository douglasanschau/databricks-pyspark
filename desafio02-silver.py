vendas = spark.read.parquet(
    "/Volumes/workspace/default/bronze/vendas"
);

clientes = spark.read.parquet(
    "/Volumes/workspace/default/bronze/clientes"
);

vendas.write \
      .mode("overwrite") \
      .parquet("/Volumes/workspace/default/silver/vendas");

clientes.write \
      .mode("overwrite") \
      .parquet("/Volumes/workspace/default/silver/clientes");