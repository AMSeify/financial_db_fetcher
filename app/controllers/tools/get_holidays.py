from datetime import date as dt
from sqlalchemy.orm import Session

from app.models.financial_db import Holiday
from config.settings import FinancialSessionLocal


def get_holiday(date: dt = None) -> bool:
    """
    Checks if the given date (or today's date if none is provided) is a holiday.
    This now also treats Thursdays and Fridays as holidays.

    Args:
        date (datetime.date, optional): The date to check for a holiday. Defaults to today's date.

    Returns:
        bool: True if the date is a holiday or if it's Thursday/Friday, False otherwise.
    """
    session = FinancialSessionLocal()
    if date is None:
        date = dt.today()

    # Check if it's Thursday or Friday
    if date.weekday() in [3, 4]:  # 3=Thursday, 4=Friday
        session.close()
        return True

    # Fetch the record for the given date from the database
    holiday_record = session.query(Holiday).filter(Holiday.Date == date).first()

    # If no record is found, return False (unless it was already Thursday/Friday above)
    if holiday_record is None:
        session.close()
        return False

    # Close the session before returning
    session.close()
    # Assuming b'1' marks a holiday
    return holiday_record.Holiday == b'1'

