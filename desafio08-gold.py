from pyspark.sql.functions import col, sum, broadcast
from pyspark.sql import Row

itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

produtos = spark.read.parquet(
    "/Volumes/workspace/default/silver/produtos"
);

produtos_mais_vendidos = (
    itens.join(
        broadcast(produtos).alias('p'), 
        on="produto_id", 
        how="inner"
    )
    .select('p.produto_id', 'quantidade')
);

produtos_mais_vendidos = produtos_mais_vendidos.groupBy('produto_id') \
                                               .agg(
                                                   sum(col("quantidade")).alias("total_vendas")
                                               ) \
                                               .orderBy(col("total_vendas").desc()) \
                                               .limit(5);

produtos_mais_vendidos.write \
                      .format("delta") \
                      .mode("overwrite") \
                      .parquet("/Volumes/workspace/default/gold/produtos_mais_vendidos");

