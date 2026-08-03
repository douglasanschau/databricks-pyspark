from pyspark.sql.functions import col, sum, broadcast

itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

produtos = spark.read.parquet(
    "/Volumes/workspace/default/silver/produtos"
);

faturamento_por_categoria = (
    itens.join(broadcast(produtos), produtos.produto_id == itens.produto_id, "inner")
    .select("categoria", "quantidade", 'preco')
);

faturamento_por_categoria = faturamento_por_categoria.withColumn("total_venda", col("quantidade") * col("preco"));

faturamento_por_categoria = faturamento_por_categoria.groupBy("categoria") \
                            .agg(sum("total_venda"));

faturamento_por_categoria.write \
     .mode("overwrite") \
     .parquet("/Volumes/workspace/default/gold/faturamento_por_categoria");
                                       

display(faturamento_por_categoria);