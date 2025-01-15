from sqlalchemy import text

from app.controllers.crawler.aiohttp_getter.aio_fetcher import time_series_to_df
from app.controllers.crawler.moment_data.js_getter import get_realtime_final_price_and_ob
from app.models.financial_db import FinalMoment, Company, OBHistory, OHLC_Summary
from app.controllers.crud.crud_operations import get_by_condition, get_all, df_bulk_insert
from datetime import datetime, timedelta
import pandas as pd
import asyncio
from config.settings import FinancialSessionLocal

pd.set_option('display.max_columns', 500)

def updateOCLH(db):
    OHLC_Summary.update_ohlc_summary(db)

# Helper function to process a batch
async def process_batch(batch, db, start_of_day, end_of_day):
    # Fetch real-time data for the batch
    data = await get_realtime_final_price_and_ob(batch)
    final, orderbook = data[0], data[1]

    # Process `final` data
    columns = ["datetime", "CompanyID", "Final", "close", "count", "volume", "MarketCap"]
    final = final[columns].copy()
    final[["Final", "close", "count", "volume", "MarketCap"]] = final[["Final", "close", "count", "volume", "MarketCap"]].astype(int)

    # condition = FinalMoment.datetime.between(start_of_day, end_of_day)
    existing_results = get_by_condition(db, FinalMoment, text(f"datetime > '{start_of_day}'"), as_dataframe=True)

    if not existing_results.empty:
        existing_results = existing_results[columns]
        existing_results[["Final", "close", "count", "volume", "MarketCap"]] = existing_results[["Final", "close", "count", "volume", "MarketCap"]].astype(int)

        # Convert datetime to string format for comparison
        date_format = "%Y-%m-%d %H:%M:%S"
        final["datetime"] = pd.to_datetime(final["datetime"]).dt.strftime(date_format)
        existing_results["datetime"] = pd.to_datetime(existing_results["datetime"]).dt.strftime(date_format)

        # Find new rows
        merged = final.merge(existing_results, on=columns, how="left", indicator=True)
        not_in_results = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        not_in_results = final

    if not not_in_results.empty:
        df_bulk_insert(db, FinalMoment, not_in_results)
        updateOCLH(db)
        print(f"Inserted {len(not_in_results)} new rows into FinalMoment.")

    # Process `orderbook` data
    columns = ["datetime", "Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price", "CompanyID"]
    orderbook[["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]] = orderbook[
        ["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]].apply(pd.to_numeric, errors="coerce", downcast="integer"
                                                                                                )
    orderbook.fillna(0, inplace=True)
    orderbook[["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]] = orderbook[
        ["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]].astype(int)

    # condition = OBHistory.datetime.between(start_of_day, end_of_day)
    existing_orderbook = get_by_condition(db, OBHistory, text(f"datetime > '{start_of_day}'"), as_dataframe=True)

    if not existing_orderbook.empty:
        existing_orderbook = existing_orderbook[columns]
        existing_orderbook[["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]] = existing_orderbook[
            ["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]].astype(int)

        # Convert datetime to string format for comparison
        date_format = "%Y-%m-%d %H:%M:%S"
        orderbook["datetime"] = pd.to_datetime(orderbook["datetime"]).dt.strftime(date_format)
        existing_orderbook["datetime"] = pd.to_datetime(existing_orderbook["datetime"]).dt.strftime(date_format)

        # Find new rows
        merged = orderbook.merge(existing_orderbook, on=columns, how="left", indicator=True)
        not_in_results = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        not_in_results = orderbook

    if not not_in_results.empty:
        df_bulk_insert(db, OBHistory, not_in_results)
        print(f"Inserted {len(not_in_results)} new rows into OBHistory.")


async def set_rt_data(batch_size=800):
    db = FinancialSessionLocal()
    companies = get_all(db, Company, as_dataframe=True)

    start_of_day = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)

    # Split companies into batches
    company_batches = [companies[i:i + batch_size] for i in range(0, len(companies), batch_size)]

    for batch in company_batches:
        print(f"Processing batch with {len(batch)} companies...")
        await process_batch(batch, db, start_of_day, end_of_day)

    updateOCLH(db)
    print("All batches processed.")


# async def main():
#     await set_rt_data(batch_size=500)  # Adjust batch size as needed

# asyncio.run(main())