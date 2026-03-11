from fastapi import Depends
from io import BytesIO
from fastapi import FastAPI, UploadFile, HTTPException, Query, File
import pandas as pd
import pandera.pandas as pa
from .database import engine, SessionLocal
from sqlalchemy.orm import Session
from .models import CleanData, FaultyData
from fastapi.responses import Response, StreamingResponse
import io
from fastapi.middleware.cors import CORSMiddleware
from .schema import orders_schema

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
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

    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="only csv files allowed")

    contents = await file.read()
    buffer = BytesIO(contents)

    try:
        df = pd.read_csv(buffer, encoding=encoding)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV read error {e}")

    finally:
        buffer.close()
        await file.close()

    if df.empty:
        raise HTTPException(status_code=400, detail="CSV has no rows")

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
        "MSRP"
    ]

    # Use Int64 for integer columns to handle NaNs correctly for Pandera
    int_cols = ["ORDERNUMBER", "QUANTITYORDERED", "ORDERLINENUMBER", "QTR_ID", "MONTH_ID", "YEAR_ID"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if col in int_cols:
            df[col] = df[col].astype("Int64")


    try:
        validate_df = orders_schema.validate(df, lazy=True)
        validate_df.to_sql(
            "clean_data",
            con=engine,
            if_exists="replace",
            index=False
        )

        return {
            "message": "CSV validated successfully",
            "total_rows": len(df),
            "valid_rows": len(validate_df),
            "faulty_rows": 0
        }

    except pa.errors.SchemaErrors as err:
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

        # Aggregate errors and insert as the FIRST column
        error_descriptions = [", ".join(error_map[idx]) for idx in error_rows]
        faulty_df.insert(0, "VALIDATION_ERRORS", error_descriptions)

        # Save Clean Data
        clean_df.to_sql(
            "clean_data",
            con=engine,
            if_exists="replace",
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
    db: Session = Depends(get_db)
):
    offset = (page - 1) * limit
    total_count = db.query(CleanData).count()
    all_data = db.query(CleanData).offset(offset).limit(limit).all()

    return {
        "data": [row.__dict__ for row in all_data],
        "total": total_count,
        "page": page,
        "limit": limit
    }


@app.get("/download/{data_type}")
async def download_data(data_type: str, db: Session = Depends(get_db)):
    if data_type == "clean":
        query = db.query(CleanData).all()
        filename = "clean_data.xlsx"
    else:
        raise HTTPException(status_code=400, detail="Invalid data type. Use 'clean' for downloading stored data.")

    if not query:
        raise HTTPException(status_code=404, detail=f"No {data_type} data found to download.")

    # Convert to list of dicts, excluding SQLAlchemy internal state
    data = []
    for row in query:
        row_dict = row.__dict__.copy()
        row_dict.pop('_sa_instance_state', None)
        data.append(row_dict)

    df = pd.DataFrame(data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    
    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


