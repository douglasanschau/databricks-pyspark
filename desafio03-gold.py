from pyspark.sql.functions import col, sum, broadcast

itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

produtos = spark.read.parquet(
    "/Volumes/workspace/default/silver/produtos"
);

produto_mais_vendido = (
    itens.join(broadcast(produtos).alias("p"), produtos.produto_id == itens.produto_id, "INNER")
    .select("p.produto_id", "quantidade")
);

produto_mais_vendido = produto_mais_vendido.groupBy("produto_id") \
                                           .agg(
                                               sum("quantidade").alias("total_vendas")
                                           );

produto_mais_vendido = produto_mais_vendido.orderBy(col("total_vendas").desc());

produto_mais_vendido.write \
                    .mode("overwrite") \
                    .parquet("/Volumes/workspace/default/gold/produto_mais_vendido");


