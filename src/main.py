from fastapi import Depends
from io import BytesIO
from fastapi import FastAPI, UploadFile, HTTPException, Query, File
import pandas as pd
import pandera as pa
from .database import engine, SessionLocal, Base
from sqlalchemy.orm import Session
from .models import CleanData, FaultyData
Base.metadata.create_all(bind=engine)
from fastapi.responses import Response, StreamingResponse
import io
import traceback
import logging
from sqlalchemy import cast, String
from fastapi.middleware.cors import CORSMiddleware
from . import schema as orders_schema_mod
from .schema import orders_schema
import warnings
from pandera.errors import SchemaErrors

warnings.filterwarnings('ignore')

# Configure logging to a file
logging.basicConfig(
    filename='app_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = FastAPI()

@app.middleware("http")
async def log_exceptions_middleware(request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        error_msg = traceback.format_exc()
        logging.error(f"Error handling request {request.url}: {error_msg}")
        raise e

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173","*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



@app.get('/')
def hello():
    return {'message': 'Hello'}


@app.post('/upload-csv')
async def upload_csv(
    file: UploadFile = File(...),
    encoding: str = Query("latin1")):

    allowed_extensions = {".csv", ".xlsx", ".xls"}
    # file_extension = file.filename.lower()[file.filename.rfind("."):]
    if not file.filename:
        raise HTTPException(status_code=400, detail="File must have a filename")

    file_extension = file.filename.lower()[file.filename.rfind("."):]
    
    if file_extension not in allowed_extensions:
        raise HTTPException(status_code=400, detail="only csv, xlsx, or xls files allowed")

    contents = await file.read()
    buffer = BytesIO(contents)

    try:
        if file_extension == ".csv":
            df = pd.read_csv(buffer, encoding=encoding)
        else:
            # For Excel files, use read_excel
            df = pd.read_excel(buffer)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"File read error {e}")

    finally:
        buffer.close()
        await file.close()

    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file has no rows")

    df["ORDERDATE"] = pd.to_datetime(df["ORDERDATE"], errors="coerce")

    numeric_cols = [
        "ORDERNUMBER",
        "QUANTITYORDERED",
        "PRICEEACH",
        "ORDERLINENUMBER",
        "SALES",
        "QTR_ID",
        "MONTH_ID",
        "YEAR_ID",
        "MSRP",
        "POSTALCODE"
    ]

    # Use Int64 for integer columns to handle NaNs correctly for Pandera
    int_cols = ["ORDERNUMBER", "QUANTITYORDERED", "ORDERLINENUMBER", "QTR_ID", "MONTH_ID", "YEAR_ID", "POSTALCODE"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if col in int_cols:
            df[col] = df[col].astype("Int64")


    try:
        # Initial validation to separate clean/faulty
        validate_df = orders_schema.validate(df, lazy=True)
        
        # --- DUPLICATE CHECK FOR ALL-CLEAN CASE ---
        existing_orders = pd.read_sql(
            "SELECT \"ORDERNUMBER\" FROM clean_data", 
            con=engine
        )["ORDERNUMBER"].tolist()
        
        is_duplicate = validate_df["ORDERNUMBER"].isin(existing_orders)
        actual_clean_df = validate_df[~is_duplicate].copy()
        duplicate_df = validate_df[is_duplicate].copy()
        
        if not actual_clean_df.empty:
            actual_clean_df.to_sql(
                "clean_data",
                con=engine,
                if_exists="append",
                index=False
            )

        if duplicate_df.empty:
            return {
                "message": "File validated successfully",
                "total_rows": len(df),
                "valid_rows": len(actual_clean_df),
                "faulty_rows": 0
            }
        else:
            # If there are duplicates, we treat them as faulty
            duplicate_df.insert(0, "VALIDATION_ERRORS", "Duplicate ORDERNUMBER")
            excel_buffer = io.BytesIO()
            duplicate_df.to_excel(excel_buffer, index=False, engine="openpyxl")
            return Response(
                content=excel_buffer.getvalue(),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": "attachment; filename=faulty_data_with_duplicates.xlsx"
                }
            )

    except SchemaErrors as err:
        failure_cases = err.failure_cases
        
        # Group errors by row index
        error_map = {}
        schema_errors = []
        
        for _, row in failure_cases.iterrows():
            if pd.isna(row["index"]):
                # This is a schema-level error (e.g. wrong column type)
                schema_errors.append(f"{row['column']}: {row['check']}")
                continue
                
            idx = int(row["index"])
            col = row["column"]
            check = row["check"]
            error_msg = f"{col}: {check}"
            if idx not in error_map:
                error_map[idx] = []
            error_map[idx].append(error_msg)
        
        error_rows = sorted(error_map.keys())
        
        # If there are NO row-level errors but there ARE schema-level errors, 
        # we treat everything as faulty or just report the schema errors.
        if not error_rows and schema_errors:
            # Fallback: if it's a column-wide error, mark all rows as having that error
            for idx in df.index:
                error_map[idx] = schema_errors
            error_rows = df.index.tolist()

        faulty_df = df.loc[error_rows].copy()
        clean_df = df.drop(index=error_rows).copy()

        # --- DUPLICATE CHECK ---
        if not clean_df.empty:
            existing_orders = pd.read_sql(
                "SELECT \"ORDERNUMBER\" FROM clean_data", 
                con=engine
            )["ORDERNUMBER"].tolist()
            
            is_duplicate = clean_df["ORDERNUMBER"].isin(existing_orders)
            duplicate_df = clean_df[is_duplicate].copy()
            clean_df = clean_df[~is_duplicate].copy()
            
            if not duplicate_df.empty:
                duplicate_df.insert(0, "VALIDATION_ERRORS", "Duplicate ORDERNUMBER")
                faulty_df = pd.concat([faulty_df, duplicate_df], ignore_index=True)
        # ------------------------

        # Aggregate errors and insert as the FIRST column
        error_descriptions = [", ".join(error_map[idx]) for idx in error_rows]
        faulty_df.insert(0, "VALIDATION_ERRORS", error_descriptions)

        # Save Clean Data
        clean_df.to_sql(
            "clean_data",
            con=engine,
            if_exists="append",
            index=False
        )

        excel_buffer = io.BytesIO()
        faulty_df.to_excel(excel_buffer, index=False, engine="openpyxl")

        return Response(
            content=excel_buffer.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=faulty_data.xlsx"
            }
        )


@app.get("/clean_data")
async def get_all_clean_data(
    page: int = Query(1, gte=1),
    limit: int = Query(10, gte=1),
    search: str = Query(None),
    db: Session = Depends(get_db)
):
    query = db.query(CleanData)

    if search:
        query = query.filter(
            CleanData.CUSTOMERNAME.ilike(f"%{search}%") |
            cast(CleanData.ORDERNUMBER, String).ilike(f"%{search}%")
        )

    total_count = query.count()
    offset = (page - 1) * limit
    all_data = query.order_by(CleanData.ORDERNUMBER.asc()).offset(offset).limit(limit).all()

    return {
        "data": [row.__dict__ for row in all_data],
        "total": total_count,
        "page": page,
        "limit": limit
    }


@app.post("/add-entry")
async def add_entry(entry: orders_schema_mod.OrderItemCreate, db: Session = Depends(get_db)):
    
    data_dict = entry.model_dump()
    df = pd.DataFrame([data_dict])
    
    
    int_cols = ["ORDERNUMBER", "QUANTITYORDERED", "ORDERLINENUMBER", "QTR_ID", "MONTH_ID", "YEAR_ID", "POSTALCODE"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    try:
       
        validated_df = orders_schema.validate(df, lazy=True)
        
        # Check if record already exists
        db_item = db.query(CleanData).filter(CleanData.ORDERNUMBER == int(entry.ORDERNUMBER)).first()
        if db_item:
            raise HTTPException(status_code=400, detail=f"Order number {entry.ORDERNUMBER} already exists")
        
        validated_df.to_sql(
            "clean_data",
            con=engine,
            if_exists="append",
            index=False
        )
        
        return {
            "message": "Entry added successfully",
            "data": data_dict
        }
        
    except SchemaErrors as err:
        failure_cases = err.failure_cases
        error_msgs = []
        for _, row in failure_cases.iterrows():
            error_msgs.append(f"{row['column']}: {row['check']}")
            
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Validation failed",
                "errors": error_msgs
            }
        )



@app.put("/edit-entry/{order_number}")
async def edit_entry(order_number: int, entry: orders_schema_mod.OrderItemCreate, db: Session = Depends(get_db)):
    # Check if record exists
    db_item = db.query(CleanData).filter(CleanData.ORDERNUMBER == order_number).first()
    if not db_item:
        raise HTTPException(status_code=404, detail="Order not found")

    # Convert Pydantic model to DataFrame for Pandera validation
    data_dict = entry.model_dump()
    df = pd.DataFrame([data_dict])
    
    # Pre-process types
    int_cols = ["ORDERNUMBER", "QUANTITYORDERED", "ORDERLINENUMBER", "QTR_ID", "MONTH_ID", "YEAR_ID", "POSTALCODE"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    try:
        # Validate
        orders_schema.validate(df, lazy=True)
        
        # Update database entry
        for key, value in data_dict.items():
            setattr(db_item, key, value)
        
        db.commit()
        db.refresh(db_item)
        
        return {
            "message": "Entry updated successfully",
            "data": data_dict
        }
        
    except SchemaErrors as err:
        failure_cases = err.failure_cases
        error_msgs = []
        for _, row in failure_cases.iterrows():
            error_msgs.append(f"{row['column']}: {row['check']}")
            
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Validation failed",
                "errors": error_msgs
            }
        )


@app.delete("/delete-entry/{order_number}")
async def delete_entry(order_number: int, db: Session = Depends(get_db)):
    # Check if record exists
    db_item = db.query(CleanData).filter(CleanData.ORDERNUMBER == order_number).first()
    if not db_item:
        raise HTTPException(status_code=404, detail="Order not found")

    # Delete the record
    db.delete(db_item)
    db.commit()

    return {
        "message": f"Entry with order number {order_number} deleted successfully"
    }

