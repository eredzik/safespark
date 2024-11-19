from pyspark.sql import functions as F

from typing_extensions import (
    
    Literal,
    LiteralString,
    
    TypeVar,
    
)

from .dataset import TColumn
T = TypeVar("T", bound=LiteralString, contravariant=True)

def col[T: LiteralString](colname: T) -> TColumn[T, T]:
    ret: TColumn[T, T] = TColumn._from_spark_col(F.col(colname))
    return ret


def lit(
    litval: str | int | float | bool | None,
) -> TColumn[Literal["lit"], Literal["lit"]]:
    ret: TColumn[Literal["lit"], Literal["lit"]] = TColumn._from_spark_col(
        F.lit(litval)
    )
    return ret