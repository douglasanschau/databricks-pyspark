from pyspark.sql.functions import col, sum, avg
from pyspark.sql import Row

itens = spark.read.parquet(
    "/Volumes/workspace/default/silver/itens"
);

produtos = spark.read.parquet(
    "/Volumes/workspace/default/silver/produtos"
);

produtos_nao_vendidos = (
    produtos.join(
        itens, 
        on="produto_id",
        how="left_anti"
    )
);

produtos_nao_vendidos.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .parquet("/Volumes/workspace/default/gold/produtos_nao_vendidos");
