clientes    = spark.table("default.clientes");
itens_venda = spark.table("default.itens_venda");
produtos    = spark.table("default.produtos");
vendas      = spark.table("default.vendas");
vendedores  = spark.table("default.vendedores");

clientes.write \
        .mode("overwrite") \
        .parquet("/Volumes/workspace/default/bronze/clientes");

itens_venda.write \
        .mode("overwrite") \
        .parquet("/Volumes/workspace/default/bronze/itens");

produtos.write \
        .mode("overwrite") \
        .parquet("/Volumes/workspace/default/bronze/produtos");

vendas.write \
      .mode("overwrite") \
      .parquet("/Volumes/workspace/default/bronze/vendas");


vendedores.write \
          .mode("overwrite") \
          .parquet("/Volumes/workspace/default/bronze/vendedores");

