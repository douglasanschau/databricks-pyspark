from pyspark.sql.functions import col, sum, broadcast

clientes = spark.read.parquet(
    "/Volumes/workspace/default/silver/clientes"
);

itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

vendas = spark.read.parquet(
    "/Volumes/workspace/default/silver/vendas"
);

cliente_mais_gastou = (
    itens.join(vendas, vendas.venda_id == itens.venda_id, "inner")
    .join(broadcast(clientes).alias("c"), clientes.cliente_id == vendas.cliente_id, "inner")
    .select("c.cliente_id", "nome", "quantidade", "valor_unitario")
);

cliente_mais_gastou = cliente_mais_gastou.withColumn("valor_total_gasto", col("quantidade") * col("valor_unitario"));

cliente_mais_gastou = cliente_mais_gastou.groupBy("cliente_id") \
                                         .agg(
                                             sum("valor_total_gasto").alias("total_gasto")
                                         );

cliente_mais_gastou = cliente_mais_gastou.orderBy(col('total_gasto').desc());

cliente_mais_gastou.write \
                   .mode("overwrite") \
                   .parquet("/Volumes/workspace/default/gold/cliente_mais_gastou");






