import sqlalchemy
import pandas as pd

from app.controllers.crawler.aiohttp_getter.aio_fetcher import fetch_multiple_js_links
from app.controllers.tools.date_tools import api_date_converter


async def get_realtime_final_price_and_ob(company_list: pd.DataFrame
) -> (pd.DataFrame, pd.DataFrame): 
    """
    Get real-time final price and order book data for a list of companies.

    :param db: SQLAlchemy database session.
    :param company_list: DataFrame containing company list.
    :return: Two DataFrames containing final price and order book data respectively.
    """
    # Generate API URLs for fetching data
    baseurl = "https://old.tsetmc.com/Loader.aspx?ParTree=151321&i={insCode}"
    company_list["InsCode"] = company_list["InsCode"].astype(str)
    links = company_list["InsCode"].apply(lambda i: baseurl.format(insCode=i)).tolist()
    # Fetch data for final prices and order book
    close_df, ob_df = await fetch_multiple_js_links(links)

    # Process final price data
    close_df["insCode"] = close_df["insCode"].astype(str)
    close_df["count"] = close_df["count"].astype(int)
    close_df["time_id"] = pd.to_numeric(close_df["time_id"], errors="coerce")
    close_df.dropna(subset=["time_id"], inplace=True)
    close_df = close_df[
        (close_df["time_id"] > 90000) & (close_df["time_id"] < 123100)
    ]
    close_df["datetime"] = pd.to_datetime(
        close_df["time_id"].apply(api_date_converter)
    )
    close_df.sort_values(by=["insCode", "count"], ascending=True, inplace=True)
    close_df["SumOfVolume"] = pd.to_numeric(
        close_df["SumOfVolume"], errors="coerce"
    ).fillna(0)
    close_df["SumOfMarketCap"] = pd.to_numeric(
        close_df["SumOfMarketCap"], errors="coerce"
    ).fillna(0)
    close_df["volume"] = close_df.groupby("insCode")["SumOfVolume"].diff().fillna(
        close_df["SumOfVolume"]
    )
    close_df["MarketCap"] = close_df.groupby("insCode")[
        "SumOfMarketCap"
    ].diff().fillna(close_df["SumOfMarketCap"])
    
    close_df = close_df.merge(
        company_list[["id", "InsCode"]], left_on="insCode", right_on="InsCode", how="left"
    )

    close_df.drop(columns=["InsCode", "insCode"], inplace=True)
    close_df.rename(columns={"id": "CompanyID"}, inplace=True)
    close_df["is_inserted"] = 0

    # Process order book data
    ob_df["insCode"] = ob_df["insCode"].astype(str)
    ob_df["time_id"] = pd.to_numeric(ob_df["time_id"], errors="coerce")
    ob_df.dropna(subset=["time_id"], inplace=True)
    ob_df["datetime"] = pd.to_datetime(ob_df["time_id"].apply(api_date_converter))
    ob_df.sort_values(by=["datetime"], ascending=True, inplace=True)
    ob_df = ob_df.merge(
        company_list[["id", "InsCode"]], left_on="insCode", right_on="InsCode", how="left"
    )
    ob_df.drop(columns=["InsCode", "insCode"], inplace=True)
    ob_df.rename(columns={"id": "CompanyID"}, inplace=True)
    ob_df = ob_df[['Depth', 'Buy_No', 'Buy_Vol', 'Buy_Price', 'Sell_Price',
                    'Sell_Vol', 'Sell_No', 'datetime', 'CompanyID']]

    return close_df, ob_df

