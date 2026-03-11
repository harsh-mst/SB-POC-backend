from fastapi import FastAPI, HTTPException, UploadFile, File, Query, Depends
import pandas as pd
from sqlalchemy.orm import Session
from .database import engine, SessionLocal
from .models import OrderItem
from .models1 import Shipment
from io import BytesIO
from sqlalchemy import func, and_
from pydantic import BaseModel
from typing import List

app = FastAPI()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
        
class CustomerStats(BaseModel):
    customername: str
    total_sales: float



@app.get("/")
def read_root():
    return {"message": "Database connected and table created"}




@app.post("/upload-csv")
async def upload_csv(
    file: UploadFile = File(...),
    encoding: str = Query("latin1")
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    contents = await file.read()
    buffer = BytesIO(contents)

    try:
        df = pd.read_csv(buffer, encoding=encoding)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV: {e}")
    finally:
        buffer.close()
        await file.close()

    if df.empty:
        raise HTTPException(status_code=400, detail="CSV has no data rows")

    df.to_sql(
        name="items5",
        con=engine,
        if_exists="replace",
        index=False,
    )

    return {"rows": int(len(df))}


@app.get("/sales-summary")
def sales_summary(db: Session = Depends(get_db)):
    total_sales = db.query(func.sum(OrderItem.sales).label("total_sales")).scalar()
    avg_order_value = db.query(func.avg(OrderItem.sales)).scalar()
    return {"total_sales": total_sales, "avg_order_value": avg_order_value}





@app.get("/top-customers")
def top_customers(limit: int = 10, db: Session = Depends(get_db)):
    customers = (
        db.query(
            OrderItem.customername, 
            func.sum(OrderItem.sales).label("total_sales")
        )
        .group_by(OrderItem.customername)
        .order_by(func.sum(OrderItem.sales).desc())
        .limit(limit)
        .all()
    )
    return [row._asdict() for row in customers]  


@app.get("/sales-by-territory")
def sales_by_territory(db: Session = Depends(get_db)):
    territory_sales = (
        db.query(
            OrderItem.territory,
            func.sum(OrderItem.sales).label("total_sales"),
            func.count(OrderItem.ordernumber).label("order_count")
        )
        .group_by(OrderItem.territory)
        .order_by(func.sum(OrderItem.sales).desc())
        .all()
    )
    return [row._asdict() for row in territory_sales]


@app.get("/top-products")
def top_products(limit: int = 10, db: Session = Depends(get_db)):
    products = (
        db.query(
            OrderItem.productcode,
            OrderItem.productline,
            func.sum(OrderItem.quantityordered).label("total_quantity"),
            func.sum(OrderItem.sales).label("total_sales")
        )
        .group_by(OrderItem.productcode, OrderItem.productline)
        .order_by(func.sum(OrderItem.quantityordered).desc())
        .limit(limit)
        .all()
    )
    return [row._asdict() for row in products]



@app.get("/high-value-customers")
def high_value_customers(db: Session = Depends(get_db)):
    high_value = (
        db.query(OrderItem.customername, func.sum(OrderItem.sales).label("total"))
        .group_by(OrderItem.customername)
        .having(func.sum(OrderItem.sales) > 10000)
        .order_by(func.sum(OrderItem.sales).desc())
        .all()
    )
    return [row._asdict() for row in high_value]


@app.get("/sales-by-country")
def sales_by_country(db: Session = Depends(get_db)):
    country_sales = (
        db.query(
            OrderItem.country,
            func.count().label("orders"),
            func.sum(OrderItem.sales).label("total_sales")
        )
        .group_by(OrderItem.country)
        .order_by(func.sum(OrderItem.sales).desc())
        .limit(20)
        .all()
    )
    return [row._asdict() for row in country_sales]


# ============================================================================================================




@app.get("/delivery-success-rate")
def delivery_success_rate(db: Session = Depends(get_db)):
    total = db.query(func.count(Shipment.id)).scalar()
    on_time = db.query(func.count(Shipment.id)).filter(Shipment.reached_on_time_y_n == 1).scalar()
    return {"on_time_rate": round(on_time / total * 100, 2), "total_shipments": total}



@app.get("/warehouse-performance")
def warehouse_performance(db: Session = Depends(get_db)):
    perf = (
        db.query(
            Shipment.warehouse_block,
            func.avg(Shipment.reached_on_time_y_n * 100).label("on_time_pct"),
            func.count().label("shipments")
        )
        .group_by(Shipment.warehouse_block)
        .order_by(func.avg(Shipment.reached_on_time_y_n).desc())
        .all()
    )
    return [row._asdict() for row in perf]




@app.get("/shipment-mode-effectiveness")
def shipment_mode_effectiveness(db: Session = Depends(get_db)):
    modes = (
        db.query(
            Shipment.mode_of_shipment,
            func.avg(Shipment.reached_on_time_y_n * 100).label("success_rate"),
            func.count().label("shipments"),
            func.avg(Shipment.cost_of_the_product).label("avg_cost")
        )
        .group_by(Shipment.mode_of_shipment)
        .order_by(func.avg(Shipment.reached_on_time_y_n).desc())
        .all()
    )
    return [row._asdict() for row in modes]




@app.get("/high-value-shipments")
def high_value_shipments(db: Session = Depends(get_db)):
    high_value = (
        db.query(Shipment)
        .filter(Shipment.cost_of_the_product > 200)
        .order_by(Shipment.cost_of_the_product.desc())
        .limit(20)
        .all()
    )
    return [item.__dict__ for item in high_value]




@app.get("/rating-vs-delivery")
def rating_vs_delivery(db: Session = Depends(get_db)):
    ratings = (
        db.query(
            Shipment.customer_rating,
            func.avg(Shipment.reached_on_time_y_n * 100).label("on_time_rate"),
            func.count().label("shipments")
        )
        .group_by(Shipment.customer_rating)
        .order_by(Shipment.customer_rating)
        .all()
    )
    return [row._asdict() for row in ratings]




@app.get("/discount-impact")
def discount_impact(db: Session = Depends(get_db)):
    discounts = (
        db.query(
            Shipment.discount_offered,
            func.avg(Shipment.reached_on_time_y_n * 100).label("success_rate"),
            func.count().label("shipments")
        )
        .group_by(Shipment.discount_offered)
        .order_by(Shipment.discount_offered)
        .all()
    )
    return [row._asdict() for row in discounts]




@app.get("/gender-delivery-stats")
def gender_delivery_stats(db: Session = Depends(get_db)):
    gender_stats = (
        db.query(
            Shipment.gender,
            func.avg(Shipment.reached_on_time_y_n * 100).label("on_time_rate"),
            func.avg(Shipment.customer_rating).label("avg_rating"),
            func.count().label("shipments")
        )
        .group_by(Shipment.gender)
        .all()
    )
    return [row._asdict() for row in gender_stats]



@app.get("/product-importance")
def product_importance_stats(db: Session = Depends(get_db)):
    importance = (
        db.query(
            Shipment.product_importance,
            func.avg(Shipment.reached_on_time_y_n * 100).label("success_rate"),
            func.count().label("shipments")
        )
        .group_by(Shipment.product_importance)
        .order_by(func.avg(Shipment.reached_on_time_y_n).desc())
        .all()
    )
    return [row._asdict() for row in importance]




@app.get("/heavy-shipments")
def heavy_shipments(db: Session = Depends(get_db)):
    heavy = (
        db.query(Shipment)
        .filter(Shipment.weight_in_gms > 2000)
        .order_by(Shipment.reached_on_time_y_n.desc(), Shipment.weight_in_gms.desc())
        .limit(50)
        .all()
    )
    return [item.__dict__ for item in heavy]



@app.get("/loyalty-vs-delivery")
def loyalty_vs_delivery(db: Session = Depends(get_db)):
    loyalty = (
        db.query(
            Shipment.prior_purchases,
            func.avg(Shipment.reached_on_time_y_n * 100).label("on_time_rate"),
            func.count().label("shipments")
        )
        .group_by(Shipment.prior_purchases)
        .order_by(Shipment.prior_purchases)
        .all()
    )
    return [row._asdict() for row in loyalty]

