
from typing_extensions import Literal
from safespark.dataset import DataFrame
from safespark import functions as F 
from pyspark.sql import DataFrame
df1: DataFrame[Literal["source1", "source2"]] = DataFrame()
df2 = df1.withColumn("c", F.col("source1"))
df3 = df2.select(F.col("source1"))
df4 = df3.withColumn("cd", F.col("source2"))  #  expect error 
df5 = df4.withColumn("cd", F.col("columnthatdoesnotexist"))  # expect error
df6 = df5.alias("abcd")
litcol = F.lit("ababdabadc").alias("litaliased")
lit2 = F.lit("alit").alias("anotherlitaliased")
expr = F.col("cd") == 1
nonexistentcol = F.col("a").alias("aliased")
existingconl = F.col("source1").alias("existent") 
cdcol = F.col("cd").alias("cd") 
df6 = df5.select(
    nonexistentcol, # expect error
    existingconl,
    cdcol,
    col("source1"),
    expr.alias('expraliased'),
    litcol,
    lit2,
)

df7 = df6.filter(expr)
df8 = df7.select(F.col("cd"))
df6.bcd  #  expect error
df6.cd

dfjoined = df6.join(df7, on=F.col('cd')==F.col('def'), how="inner")

