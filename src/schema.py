import pandera.pandas as pa
from pandera import Check, DataFrameSchema, Column
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


orders_schema = DataFrameSchema({
    "ORDERNUMBER": Column(int, nullable=False),
    "QUANTITYORDERED": Column(int, Check.gt(0)),
    "PRICEEACH": Column(float, Check.gt(0)),
    "ORDERLINENUMBER": Column(int, Check.ge(1)),
    "SALES": Column(float, Check.gt(0)),
    "ORDERDATE": Column(pa.DateTime),
    "STATUS": Column(str),
    "QTR_ID": Column(int, Check.isin([1, 2, 3, 4])),
    "MONTH_ID": Column(int, Check.in_range(1, 12)),
    "YEAR_ID": Column(int),
    "PRODUCTLINE": Column(str),
    "MSRP": Column(float, Check.gt(0)),
    "PRODUCTCODE": Column(str),
    "CUSTOMERNAME": Column(str),
    "PHONE": Column(str),
    "ADDRESSLINE1": Column(str),
    "ADDRESSLINE2": Column(str, nullable=True),
    "CITY": Column(str),
    "STATE": Column(str, nullable=True),
    "POSTALCODE": Column(int, nullable=True),
    "COUNTRY": Column(str),
    "TERRITORY": Column(str, nullable=True),
    "CONTACTLASTNAME": Column(str),
    "CONTACTFIRSTNAME": Column(str),
    "DEALSIZE": Column(
        str,
        Check.isin(["Small", "Medium", "Large"])
    )
})


class OrderItemCreate(BaseModel):
    ORDERNUMBER: int
    QUANTITYORDERED: int
    PRICEEACH: float
    ORDERLINENUMBER: int
    SALES: float
    ORDERDATE: datetime
    STATUS: str
    QTR_ID: int
    MONTH_ID: int
    YEAR_ID: int
    PRODUCTLINE: str
    MSRP: float
    PRODUCTCODE: str
    CUSTOMERNAME: str
    PHONE: str
    ADDRESSLINE1: str
    ADDRESSLINE2: Optional[str] = None
    CITY: str
    STATE: Optional[str] = None
    POSTALCODE: Optional[int] = None
    COUNTRY: str
    TERRITORY: Optional[str] = None
    CONTACTLASTNAME: str
    CONTACTFIRSTNAME: str
    DEALSIZE: str
