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

async def set_rt_data():
    db = FinancialSessionLocal()
    companies = get_all(db, Company, as_dataframe=True)

    start_of_day = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)

    condition = FinalMoment.datetime.between(start_of_day, end_of_day)
    results = get_by_condition(db, FinalMoment, condition, as_dataframe=True)

    print("Existing results:")

    data = await get_realtime_final_price_and_ob(companies)
    final = data[0]
    final = final[['datetime', "CompanyID", "Final", "close", "count", 'volume', 'MarketCap']]


    # Ensure both DataFrames have the same columns and data types
    columns = ["datetime", "CompanyID", "Final", "close", "count", "volume", "MarketCap"]
    final = final[columns].copy()
    final[["Final", "close", "count", "volume", "MarketCap"]] = final[["Final", "close", "count", "volume", "MarketCap"]].astype(int)

    if not results.empty:
        results = results[columns]
        results[["Final", "close", "count", "volume", "MarketCap"]] = results[["Final", "close", "count", "volume", "MarketCap"]].astype(int)

        # Convert datetime to string format for both DataFrames
        date_format = "%Y-%m-%d %H:%M:%S"
        final["datetime"] = pd.to_datetime(final["datetime"]).dt.strftime(date_format)
        results["datetime"] = pd.to_datetime(results["datetime"]).dt.strftime(date_format)

        # Merge to find new rows
        merged = final.merge(results,
                             on=columns,
                             how='left',
                             indicator=True)
        not_in_results = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        # If `results` is empty, all rows in `final` are new
        not_in_results = final


    # Insert new rows into the database
    if not not_in_results.empty:
        df_bulk_insert(db, FinalMoment, not_in_results)
        print(f"Inserted {len(not_in_results)} new rows into the database.")
        updateOCLH(db)
    else:
        print("No new data to insert.")

    orderbook = data[1]

    condition = OBHistory.datetime.between(start_of_day, end_of_day)
    results = get_by_condition(db, OBHistory, condition, as_dataframe=True)

    # Safely convert to integers
    orderbook[["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]] = orderbook[
        ["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]].apply(pd.to_numeric, errors='coerce', downcast='integer')

    # Handle NaN values that might have been introduced by coercion
    orderbook.fillna(0, inplace=True)

    # Convert the columns to integers
    orderbook[["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]] = orderbook[
        ["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]].astype(int)
    columns = ["datetime","Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price","CompanyID"]
    if not results.empty:
        results = results[columns]
        results[["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]] = results[["Depth", "Sell_No", "Sell_Vol", "Sell_Price", "Buy_No", "Buy_Vol", "Buy_Price"]].astype(int)

        # Convert datetime to string format for both DataFrames
        date_format = "%Y-%m-%d %H:%M:%S"
        orderbook["datetime"] = pd.to_datetime(orderbook["datetime"]).dt.strftime(date_format)
        results["datetime"] = pd.to_datetime(results["datetime"]).dt.strftime(date_format)


# Merge to find new rows
        merged = orderbook.merge(results,
                             on=columns,
                             how='left',
                             indicator=True)
        not_in_results = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        # If `results` is empty, all rows in `orderbook` are new
        not_in_results = orderbook
        print("New rows to be inserted:")
    # print(not_in_results)

    # Insert new rows into the database
    if not not_in_results.empty:
        df_bulk_insert(db, OBHistory, not_in_results)
        print(f"Inserted {len(not_in_results)} new rows into the database.")
    else:
        print("No new data to insert.")
