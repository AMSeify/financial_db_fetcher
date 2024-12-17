# app/controllers/crud.py
from sqlalchemy.orm import Session
from pandas import DataFrame
from typing import Type, List
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

BATCH_SIZE = 2000

def get_all(db: Session, model: Type, as_dataframe: bool = False):
    """Fetch all records from the database."""
    records = db.query(model).all()
    if as_dataframe:
        return to_dataframe(records)
    return records

def get_by_id(db: Session, model: Type, record_id: int, as_dataframe: bool = False):
    """Fetch a single record by ID."""
    record = db.query(model).filter(model.id == record_id).first()
    if as_dataframe:
        return to_dataframe([record]) if record else DataFrame()
    return record

def get_by_condition(db: Session, model: Type, condition, as_dataframe: bool = False):
    """Fetch records based on a condition."""
    records = db.query(model).filter(condition).all()
    if as_dataframe:
        return to_dataframe(records)
    return records

def update(db: Session, model: Type, record_id: int, updates: dict):
    """Update a record based on the given updates dictionary."""
    record = db.query(model).filter(model.id == record_id).first()
    if not record:
        print(f"Record with id {record_id} not found.")
        return None
    for key, value in updates.items():
        if hasattr(record, key):
            setattr(record, key, value)
    db.commit()
    db.refresh(record)
    return record


def delete(db: Session, model: Type, record_id: int):
    """Delete a record by ID."""
    record = db.query(model).filter(model.id == record_id).first()
    if record:
        db.delete(record)
        db.commit()
        return True
    return False

def bulk_insert(db: Session, model: Type, records: List[dict]):
    """Insert records in bulk with a batch size of 2000 and return inserted records."""
    inserted_records = []
    try:
        # Wrap the insertion process with tqdm to show progress
        for i in tqdm(range(0, len(records), BATCH_SIZE), desc="Bulk inserting records"):
            batch = records[i:i + BATCH_SIZE]

            # Bulk insert the batch
            db.bulk_insert_mappings(model, batch)

            # Flush the session to ensure the batch is prepared for commit
            db.flush()

            # Commit after each batch
            db.commit()

        # Fetch the inserted records (assuming 'id' is the primary key)
        inserted_records = db.query(model).order_by(model.id.desc()).limit(len(records)).all()
    except IntegrityError as e:
        db.rollback()  # Rollback the session on error
        print(f"Error in bulk insert: {e}")
    finally:
        db.close()  # Ensure session is closed after the process
    return inserted_records


def to_dataframe(records: List[Type]) -> DataFrame:
    """Convert SQLAlchemy model records to a Pandas DataFrame."""
    if not records:
        return DataFrame()

    # Get the column names dynamically
    columns = inspect(records[0].__class__).columns.keys()
    data = [{col: getattr(record, col) for col in columns} for record in records]
    return DataFrame(data)

def df_bulk_insert(db: Session, model: Type, df: DataFrame):
    """Insert records in bulk from a DataFrame with progress tracking."""
    # Convert the DataFrame to a list of dictionaries
    records = df.to_dict(orient='records')

    # Insert in batches with progress bar
    try:
        for i in tqdm(range(0, len(records), BATCH_SIZE), desc="Bulk inserting records"):
            batch = records[i:i + BATCH_SIZE]

            # Bulk insert the batch
            db.bulk_insert_mappings(model, batch)

            # Commit after each batch
            db.commit()

        # Optionally, fetch inserted records (if you want to return them)
        inserted_records = db.query(model).order_by(model.id.desc()).limit(len(records)).all()
        return inserted_records

    except IntegrityError as e:
        db.rollback()
        print(f"Error during bulk insert: {e}")
        return None