from pyspark.sql.functions import col, sum

itens = spark.read.parquet(
    "/Volumes/workspace/default/bronze/itens"
);

produtos = spark.read.parquet(
    "/Volumes/workspace/default/bronze/produtos"
);

itens = itens.dropDuplicates();
itens = itens.na.drop(subset=["valor_unitario"]); 
itens =  itens.na.drop(subset=["quantidade"]); 

produtos = produtos.dropDuplicates();
produtos = produtos.na.drop(subset=["categoria"]);
produtos = produtos.na.drop(subset=["preco"]);

itens.write \
     .mode("overwrite") \
     .parquet("/Volumes/workspace/default/silver/itens");

produtos.write \
     .mode("overwrite") \
     .parquet("/Volumes/workspace/default/silver/produtos");
