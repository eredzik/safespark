from safespark.generate import generate_from_parquet
def test_generate_from_parquet():
    declaration = generate_from_parquet("safespark/tests/test_datasets/iris/spark/iris.parquet")
    assert declaration == 'Dataset[Literal["sepal.length", "sepal.width", "petal.length", "petal.width", "variety"]]'

    