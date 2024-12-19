import asyncio

import pandas as pd
from sqlalchemy import text, false

from app.controllers.crawler.daily_data.fetch_daily_data import updateOCLHSummery, fetch_daily_data
from app.controllers.crawler.historical_checker.final_getter import get_final_price_history
from app.controllers.crawler.historical_checker.ob_getter import get_orderbook_history
from app.controllers.crud.crud_operations import get_by_condition, get_all, df_bulk_insert
from app.models.financial_db import DataStatus, Company, FinalMoment, OBHistory
from config.settings import FinancialSessionLocal


def update_is_checked_to_true(db):
    """Update the is_checked column of all records in the data_status table to True."""
    try:
        db.query(DataStatus).update({DataStatus.is_checked: True})
        db.commit()
        print("All records updated successfully.")
    except Exception as e:
        db.rollback()
        print(f"Failed to update records: {e}")

async def DownloadMissing():
    db = FinancialSessionLocal()
    companies = get_all(db , Company , as_dataframe=True)
    await updateOCLHSummery()

    for _ , row in companies.iterrows():

        ### Fitting the Final ###

        ins_code = row.InsCode
        df = get_by_condition(db , DataStatus , condition=text(f"company_id = {row.id} and is_checked = 0 and (orderbook = 0 or moment=0 or ohlc=0 or final=0)") ,as_dataframe=True)
        if not df.empty:
            final_df = df[df.final == False]
            final_df = final_df[['date','company_id','final']]
            final_df['date'] = pd.to_datetime(final_df['date'])
            final_df['dateid'] = final_df['date'].dt.strftime('%Y%m%d')
            print(f"downloading final data for {row.ticker}")
            insert_data = await get_final_price_history(ins_code , final_df['dateid'].to_list())
            insert_data['CompanyID'] = row.id
            print(f"inserting {len(insert_data)} records for {row.ticker}")
            try:
                df_bulk_insert(db , FinalMoment , insert_data)
            except Exception as e:
                print(f"Failed to insert records: {e}")
            del final_df , insert_data
            ###########################

            ### Fitting the Orderbook ###
            ob_df = df[df.orderbook == False]
            ob_df = ob_df[['date','company_id','orderbook']]
            ob_df['date'] = pd.to_datetime(ob_df['date'])
            ob_df['dateid'] = ob_df['date'].dt.strftime('%Y%m%d')
            print(f"downloading orderbook data for {row.ticker}")
            insert_data = await get_orderbook_history(ins_code , ob_df['dateid'].to_list())
            insert_data['CompanyID'] = row.id
            print(f"inserting {len(insert_data)} records for {row.ticker}")
            try:
                df_bulk_insert(db , OBHistory , insert_data)
            except Exception as e:
                print(f"Failed to insert records: {e}")
            del ob_df , insert_data
            ##########################

            # ### Fitting the Orderbook ###
            # moment_df = df[df.moment == False]
            # moment_df_df = moment_df[['date','company_id','moment']]
            # moment_df['date'] = pd.to_datetime(moment_df['date'])
            # moment_df['dateid'] = moment_df['date'].dt.strftime('%Y%m%d')
            # print(f"downloading orderbook data for {row.ticker}")
            # insert_data = await get_orderbook_history(ins_code , moment_df['dateid'].to_list())
            # insert_data['CompanyID'] = row.id
            # print(f"inserting {len(insert_data)} records for {row.ticker}")
            # df_bulk_insert(db , OBHistory , insert_data)
            # del moment_df , insert_data
            ###########################
    else:
        print("no history glitch found")

    update_is_checked_to_true(db)

