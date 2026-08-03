from pyspark.sql.functions import col, sum, avg

itens_vendas = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

itens_vendas = itens_vendas.withColumn("valor_total", col('quantidade') * col('valor_unitario'));

ticket_medio_venda = itens_vendas.groupBy('venda_id') \
                            .agg(
                                avg(col("valor_total")).alias("ticket_medio")
                            );

ticket_medio_venda.write \
            .format("delta") \
            .mode("overwrite") \
            .parquet("/Volumes/workspace/default/gold/ticket_medio_venda");