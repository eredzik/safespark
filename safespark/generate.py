import pyarrow.parquet
def generate_from_parquet(parquet_path:str):
    schema=pyarrow.parquet.read_schema(parquet_path, memory_map=True)
    names =', '.join([f'"{name}"' for name in schema.names])
    declaration = f"Dataset[Literal[{names}]]"
    return declaration
    

