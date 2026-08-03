from pyspark.sql.functions import col, sum, broadcast

vendedores = spark.read.parquet(
    "/Volumes/workspace/default/silver/vendedores"
);


itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

vendas = spark.read.parquet(
    "/Volumes/workspace/default/silver/vendas"
);


faturamento_por_vendedor = (
    itens.join(vendas.alias("v"), vendas.venda_id == itens.venda_id, "inner")
    .join(broadcast(vendedores).alias("vend"), vendedores.vendedor_id == vendas.vendedor_id, 'inner')
    .select("vend.vendedor_id", "quantidade", "valor_unitario")
);

faturamento_por_vendedor = faturamento_por_vendedor.withColumn("total_venda", col("quantidade") * col("valor_unitario"));

faturamento_por_vendedor = faturamento_por_vendedor.groupBy("vendedor_id") \
                                                   .agg(
                                                      sum("total_venda").alias("total_faturamento")  
                                                   );

faturamento_por_vendedor.write \
                        .mode("overwrite") \
                        .parquet("/Volumes/workspace/default/gold/faturamento_por_vendedor");



