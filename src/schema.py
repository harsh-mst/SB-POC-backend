import pandera.pandas as pa
from pandera import Check, DataFrameSchema, Column


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
