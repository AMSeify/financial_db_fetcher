from typing import List

import pandas as pd

from app.controllers.crawler.aiohttp_getter.aio_fetcher import time_series_to_df
from app.controllers.tools.date_tools import api_date_converter


async def get_final_price_history(ins_code: str, day_ids: List[str]) -> pd.DataFrame:
    urls = [(f'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceHistory/'
             f'{ins_code}/{day_id}') for day_id in day_ids]

    final_data = await time_series_to_df(urls, 'closingPriceHistory')
    if not final_data.empty:
        hEven = final_data['hEven'].astype(int)
        final_data = final_data[(hEven > 90000) & (hEven < 123100)]
        try:
            final_data['datetime'] = pd.to_datetime(
                final_data.apply(lambda row: api_date_converter(dEven=row.dEven, hEven=int(row.hEven)), axis=1)
            )

            final_data.rename({'pClosing': 'Final', 'pDrCotVal': 'close', 'zTotTran': 'count',
                               'qTotTran5J': 'SumVolume', 'qTotCap': 'SumMarketCap'}, axis='columns', inplace=True)

            final_data.sort_values(by=['count', 'datetime'], ascending=True, inplace=True)
            # final_data:pd.DataFrame
            final_data.sort_values(['dEven' , 'hEven'], ascending=True, inplace=True)
            # print(final_data['SumVolume'] - final_data['SumVolume'].shift(1))
            final_data['volume'] = final_data.groupby('dEven')['SumVolume'].diff().fillna(0)
            final_data['MarketCap'] = final_data.groupby('dEven')['SumMarketCap'].diff().fillna(0)

            final_data = final_data[['datetime', 'Final', 'close', 'count', 'volume', 'MarketCap']]
            return final_data
        except:
            print(final_data)
            return pd.DataFrame()
    else:
        return pd.DataFrame()