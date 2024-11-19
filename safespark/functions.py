from pyspark.sql import functions as F

from typing_extensions import (
    
    Literal,
    LiteralString,
    
    TypeVar,
    
)
from pyspark.sql.functions import *
from .dataset import TColumn, ColumnOrColname, Out, T

# T = TypeVar("T", bound=LiteralString, contravariant=True)

def col(colname: T) -> TColumn[T, T]:
    ret: TColumn[T, T] = TColumn._from_spark_col(F.col(colname))
    return ret


def lit(
    litval: str | int | float | bool | None,
) -> TColumn[Literal["lit"], Literal["lit"]]:
    ret: TColumn[Literal["lit"], Literal["lit"]] = TColumn._from_spark_col(
        F.lit(litval)
    )
    return ret

def trim(
    col: ColumnOrColname[T, Out]
)-> TColumn[T, Literal["expr"]]:
    ret: TColumn[T, Literal["expr"]] = TColumn._from_spark_col(F.trim(col))
    return ret

def year(col: ColumnOrColname[T, Out]
)-> TColumn[T, Literal["expr"]]:
    ret: TColumn[T, Literal["expr"]] = TColumn._from_spark_col(F.year(col))
    return ret