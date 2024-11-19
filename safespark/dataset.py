import datetime
import decimal
from typing import (
    Any,
    Generic,
    Literal,
    LiteralString,
    TypeAlias,
    TypeVar,
    Union,
    override,
)

from pyspark.sql import DataFrame
from pyspark.sql.column import Column

T = TypeVar("T", bound=LiteralString, contravariant=True)
TAlias = TypeVar("TAlias", bound=LiteralString, covariant=True)
T2 = TypeVar("T2", bound=LiteralString, covariant=True)
In = TypeVar("In", bound=LiteralString, covariant=True)
Out = TypeVar("Out", bound=LiteralString, covariant=True)
InOther = TypeVar("InOther", bound=LiteralString)
OutOther = TypeVar("OutOther", bound=LiteralString)
OutLit = TypeVar("OutLit", bound=LiteralString)
TSource = TypeVar("TSource", bound=LiteralString)
TResult = TypeVar("TResult", bound=LiteralString)
PrimitiveType = Union[bool, float, int, str]

DecimalLiteral = decimal.Decimal
DateTimeLiteral = Union[datetime.datetime, datetime.date]
LiteralType = PrimitiveType


class TColumn(Generic[In, Out], Column):
    @classmethod
    def _from_spark_col(cls, col: Column) -> "TColumn[In, Out]":
        new = cls(col._jc)
        return new

    @override
    def alias(self, alias: T2, *args: Any, **kwargs: Any) -> "TColumn[In, T2]":
        newcol: TColumn[In, T2] = TColumn._from_spark_col(super().alias(alias))
        return newcol

    def __eq__(
        self,
        other: LiteralType
        | DateTimeLiteral
        | "TColumn[InOther, OutOther]"
        | DecimalLiteral,
    ):  # type: ignore[override]
        if isinstance(other, TColumn):
            newcol_column: TColumn[Union[InOther, In], Literal["expr"]] = (
                TColumn._from_spark_col(super().__eq__(other))
            )
            return newcol_column
        else:
            newcol: TColumn[In, Literal["expr"]] = TColumn._from_spark_col(
                super().__eq__(other)
            )
            return newcol




ColumnOrColname: TypeAlias = Union[TColumn[Union[T2, Literal["lit"]], Out], Out]


class Dataset(Generic[T], DataFrame):
    @classmethod
    def _fromSpark(cls, df: DataFrame) -> "Dataset[T]":
        return cls(jdf=df._jdf, sql_ctx=df.sparkSession)

    def withColumn(  # type: ignore
        self, colname: T2, col: TColumn[T, Out]
    )-> "Dataset[Union[T, T2]]":
        # newcols: list[Union[ColumnOrColumnName[T], T2]] = [self.columns_t, colname]
        res: Dataset[Union[T, T2]] = Dataset._fromSpark(
            super().withColumn(colname, col)
        )
        return res

    @override
    def alias(self, alias: TAlias) -> "Dataset[TAlias+T]":  # type: ignore
        
        return Dataset._fromSpark(self.alias(alias))
        

    def join(
            self,
            other: "Dataset[T2]",
            on: TColumn[Union[T, T2], Any],
            how: Literal["inner", "left", "right", "full", "cross"],
            ) -> "Dataset[Union[T, T2]]":
        res: Dataset[Union[T, T2]] = Dataset._fromSpark(
            super().join(other, using, how)
        )
        return res
    
    def select(   # type: ignore
        self,
        *colnames: Union[
            TColumn[T, Out],
            TColumn[Literal["lit"], OutLit],
        ],
    ) -> "Dataset[Union[Out, OutLit]]": 
        res: Dataset[Union[Out, OutLit]] = self._fromSpark(super().select(colnames))
        return res

    def filter(self, *conditions: 
            TColumn[Union[T, Literal["lit"]], Out],
            
        ) -> "Dataset[T]":
        res: Dataset[T] = self._fromSpark(super().filter(conditions))
        return res
    

    def __getattr__(self, name: T) -> TColumn[T, T]:  
        rescol : TColumn[T, T] = TColumn._from_spark_col(super().__getattr__(name))
        return rescol