from safespark.generate import generate_from_parquet
def test_generate_from_parquet():
    declaration = generate_from_parquet("safespark/tests/test_DataFrames/iris/spark/iris.parquet")
    assert declaration == 'DataFrame[Literal["sepal.length", "sepal.width", "petal.length", "petal.width", "variety"]]'

    