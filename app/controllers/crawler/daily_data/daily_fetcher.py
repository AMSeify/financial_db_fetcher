import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from jdatetime import date as jdate
import asyncio
import swifter
from tqdm import tqdm
from app.controllers.crawler.aiohttp_getter.aio_fetcher import time_series_to_df
from app.controllers.tools.date_tools import api_date_converter
from app.models.financial_db import DailyPrice

history_url_template = 'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{ins_code}/0'

async def get_daily_price_history(ins_codes: dict[str, str]) -> pd.DataFrame:
    urls = [history_url_template.format(ins_code=ins_code) for ins_code in ins_codes.values()]
    datas = await tqdm_asyncio.gather(*[time_series_to_df([url], 'closingPriceDaily') for url in tqdm(urls, desc="Fetching data")])
    columns = [col.name for col in DailyPrice.__table__.columns]
    all_dataframes = []

    for (company_id, ins_code), data in zip(ins_codes.items(), datas):
        try:
            data = data.rename(columns={
                'priceFirst': 'open',
                'priceMax': 'high',
                'priceMin': 'low',
                'pDrCotVal': 'close',
                'pClosing': 'final',
                'priceYesterday':'y_final',
                'qTotTran5J': 'volume',
                'qTotCap': 'value',
                'zTotTran': 'count',
                'dEven': 'date',
            })
            data['CompanyID'] = company_id
            all_dataframes.append(data)
        except Exception as e:
            pass
    daily_price = pd.concat(all_dataframes, ignore_index=True)
    # Convert Gregorian date to string YYYY-MM-DD
    daily_price['date'] = pd.to_datetime(daily_price['date'].apply(lambda x: api_date_converter(dEven=x))).dt.strftime('%Y-%m-%d')

    # Use Swifter to speed up jdate conversion
    daily_price['jdate'] = daily_price['date'].swifter.apply(
        lambda x: jdate.fromgregorian(date=pd.to_datetime(x)).strftime('%Y-%m-%d')
    )

    daily_price = daily_price[['id', 'CompanyID', 'date', 'open', 'high', 'low', 'close', 'final','y_final' , 'volume', 'value', 'count', 'jdate']]

    return daily_price
