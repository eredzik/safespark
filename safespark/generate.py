import pyarrow.parquet
from pyspark.sql import DataFrame
def generate_from_parquet(parquet_path:str):
    schema=pyarrow.parquet.read_schema(parquet_path, memory_map=True)
    names =', '.join([f'"{name}"' for name in schema.names])
    declaration = f"DataFrame[Literal[{names}]]"
    return declaration
    

def generate_from_spark(spark_df:DataFrame):
    names =', '.join([f'"{name}"' for name in spark_df.columns])
    declaration = f"DataFrame[Literal[{names}]]"
    return declaration