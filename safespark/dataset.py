import datetime
import decimal
from typing import List, Tuple
from typing_extensions import (
    Any,
    Generic,
    Literal,
    LiteralString,
    TypeAlias,
    TypeVar,
    Union,
    override,
)

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DataType
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

LooseOther: TypeAlias = (
    LiteralType | DateTimeLiteral | "TColumn[InOther, OutOther]" | DecimalLiteral
)


class TColumn(Generic[In, Out], Column):
    @classmethod
    def _from_spark_col(cls, col: Column) -> "TColumn[In, Out]":
        new = cls(col._jc)
        return new

    @override
    def alias(self, alias: T2, *args: Any, **kwargs: Any) -> "TColumn[In, T2]":
        newcol: TColumn[In, T2] = TColumn._from_spark_col(super().alias(alias))
        return newcol


    def __binary_op(self, other: LooseOther[InOther, OutOther], op: str):
        if isinstance(other, TColumn):
            newcol_column: TColumn[Union[InOther, In], Literal["expr"]] = (
                TColumn._from_spark_col(super().__dict__[op](other))
            )
            return newcol_column
        else:
            newcol: TColumn[In, Literal["expr"]] = TColumn._from_spark_col(
                super().__dict__[op](other)
            )
            return newcol
    def __eq__( # type: ignore[override]
        self,
        other: LooseOther[InOther, OutOther],
    ): 
        return self.__binary_op(other, "__eq__")
    def __and__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__and__")

    def __sub__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__sub__")
    def __mul__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__mul__")
    def __truediv__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__truediv__")
    def __mod__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__mod__")
    def __add__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__add__")
    def __floordiv__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__floordiv__")
    def __pow__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__pow__")
    def __lshift__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__lshift__")
    def __rshift__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__rshift__")
    def __xor__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__xor__")
    def __or__(self, other: LooseOther[InOther, OutOther]):
        return self.__binary_op(other, "__or__")

    def cast(self, dataType: DataType | str) -> "TColumn[In, Out]":
        newcol: TColumn[In, Out] = TColumn._from_spark_col(super().cast(dataType))
        return newcol


ColumnOrColname: TypeAlias = Union[TColumn[Union[T2, Literal["lit"]], Out], Out]


class DataFrame(Generic[T], SparkDataFrame):
    @classmethod
    def _fromSpark(cls, df: SparkDataFrame) -> "DataFrame[T]":
        return cls(jdf=df._jdf, sql_ctx=df.sparkSession)

    def withColumn(  # type: ignore
        self, colname: T2, col: TColumn[T, Out]
    ) -> "DataFrame[Union[T, T2]]":
        # newcols: list[Union[ColumnOrColumnName[T], T2]] = [self.columns_t, colname]
        res: DataFrame[Union[T, T2]] = DataFrame._fromSpark(
            super().withColumn(colname, col)
        )
        return res

    def alias(self, alias: TAlias) -> "DataFrame[T]":  # type: ignore
        newdf:"DataFrame[T]" = DataFrame._fromSpark(super().alias(alias))
        return newdf

    def join(
        self,
        other: "DataFrame[T2]",
        on: TColumn[Union[T, T2], Any],
        how: Literal["inner", "left", "right", "full", "cross"],
    ) -> "DataFrame[Union[T, T2]]":
        res: DataFrame[Union[T, T2]] = DataFrame._fromSpark(
            super().join(other, on, how)
        )
        return res

    def select(  # type: ignore
        self,
        *colnames: Union[
            # T,
            TColumn[T, Out],
            TColumn[Literal["lit"], OutLit],
        ],
    ) -> "DataFrame[Union[Out, OutLit]]":
        res: DataFrame[Union[Out, OutLit]] = self._fromSpark(super().select(colnames))
        return res

    def filter(
        self,
        *conditions: TColumn[Union[T, Literal["lit"]], Out],
    ) -> "DataFrame[T]":
        res: DataFrame[T] = self._fromSpark(super().filter(conditions))
        return res

    @property
    def columns(self) -> Tuple[TColumn[T, T], ...]:
        return self.columns

    def __getitem__(self, key: T) -> TColumn[T, T]:
        return self.columns[key]

    def __getattr__(self, name: T) -> TColumn[T, T]:
        rescol: TColumn[T, T] = TColumn._from_spark_col(super().__getattr__(name))
        return rescol
