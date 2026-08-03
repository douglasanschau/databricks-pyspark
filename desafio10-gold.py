from pyspark.sql.functions import col, sum, broadcast, date_format

vendas = spark.read.parquet(
    "/Volumes/workspace/default/silver/vendas"
);

itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

faturamento_mensal = (
    itens.join(
        vendas.alias("v"),
        on="venda_id",
        how="inner"
    ) \
    .select(
        "v.venda_id",
        date_format("v.data", "yyyy").alias("ano"), 
        date_format("v.data", "MM").alias("mes"),  
        date_format("v.data", "yyyy-MM").alias("mes_ano"),
        "valor_unitario", 
        "quantidade"
    )
)

faturamento_mensal = faturamento_mensal.withColumn("valor_total", col("valor_unitario") * col("quantidade"));

faturamento_mes_ano = faturamento_mensal.groupBy("mes_ano") \
                                        .agg(
                                           sum("valor_total").alias("faturamento_mes")
                                        ) \
                                        .orderBy(col('mes_ano').desc());

faturamento_mes_ano.write \
                   .format("delta") \
                   .mode("overwrite") \
                   .parquet("/Volumes/workspace/default/gold/faturamento_mes_ano");

faturamento_mensal.write \
                  .partitionBy("ano", "mes") \
                  .format("delta") \
                  .mode("overwrite") \
                  .parquet("/Volumes/workspace/default/gold/faturamento_mensal");


