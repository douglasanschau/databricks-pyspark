from pyspark.sql.functions import col, sum, broadcast

clientes = spark.read.parquet(
    "/Volumes/workspace/default/silver/clientes"
);

vendas = spark.read.parquet(
    "/Volumes/workspace/default/silver/vendas"
);

itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

faturamento_por_estado = (
    itens.join(
        vendas,
        on="venda_id",
        how="inner"
    )
    .join(
        broadcast(clientes).alias("c"),
        on="cliente_id",
        how="inner"
    )
    .select(
        "c.estado",
        "quantidade",
        "valor_unitario"
    )
);

faturamento_por_estado = faturamento_por_estado.withColumn("valor_total", col("quantidade") * col("valor_unitario"));

faturamento_por_estado = faturamento_por_estado.groupBy("estado") \
                                               .agg(
                                                   sum(col("valor_total")).alias("total_faturado")
                                               );

faturamento_por_estado.write \
                      .format("delta") \
                      .mode("overwrite") \
                      .parquet("/Volumes/workspace/default/gold/faturamento_por_estado");
