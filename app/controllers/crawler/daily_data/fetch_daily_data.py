import asyncio
import pandas as pd
from sqlalchemy import text
from app.controllers.crawler.daily_data.daily_fetcher import get_daily_price_history
from app.controllers.crud.crud_operations import get_all, get_by_condition, df_bulk_insert
from app.models.financial_db import Company, DailyPrice, OHLC_Summary, DataStatus
from config.settings import FinancialSessionLocal
import pandas as pd

pd.set_option('display.max_columns', 500)
async def fetch_daily_data():
    db = FinancialSessionLocal()
    companies = get_all(db, Company, as_dataframe=True)
    companies: pd.DataFrame

    for _, company in companies.iterrows():
        co_daily = await get_daily_price_history({company.id: company.InsCode})
        db_daily = get_by_condition(db, DailyPrice, condition=text(f"CompanyID = {company.id}"), as_dataframe=True)

        # Use ~ and .isin() to filter rows
        db_daily['date'] = pd.to_datetime(db_daily['date'])
        co_daily['date'] = pd.to_datetime(co_daily['date'])

        co_daily = co_daily[~co_daily['date'].isin(db_daily['date'])]

        co_daily['date'] = co_daily['date'].dt.strftime('%Y-%m-%d')
        co_daily['id'] = None
        print(co_daily)
        df_bulk_insert(db , DailyPrice , co_daily)


    db.close()  # Close the database session

async def updateOCLHSummery():
    db = FinancialSessionLocal()
    DataStatus.populate_data_status(db)
    db.close()  # Close the database session
