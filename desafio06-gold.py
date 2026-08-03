from pyspark.sql.functions import col, sum, avg

vendas = spark.read.parquet(
    "/Volumes/workspace/default/silver/vendas"
);

clientes = spark.read.parquet(
    "/Volumes/workspace/default/silver/clientes"
);

clientes = spark.read.parquet("/Volumes/workspace/default/silver/clientes")

clientes_sem_venda = (
    clientes.join(
        vendas,
        on="cliente_id",
        how="left_anti"
    )
);

clientes_sem_venda.write \
                  .format("delta") \
                  .mode("overwrite") \
                  .parquet("/Volumes/workspace/default/gold/clientes_sem_venda");
