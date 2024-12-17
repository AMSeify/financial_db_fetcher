from typing import List

import pandas as pd

from app.controllers.crawler.aiohttp_getter.aio_fetcher import time_series_to_df
from app.controllers.tools.date_tools import api_date_converter


async def get_orderbook_history(ins_code: str, day_ids: List[str]) -> pd.DataFrame:
    urls = [(f'https://cdn.tsetmc.com/api/BestLimits/'
             f'{ins_code}/{day_id}') for day_id in day_ids]

    ob_data = await time_series_to_df(urls, 'bestLimitsHistory')
    # prangeDF = await time_series_to_df(pRangeUrl, 'closingPriceDaily', extra_data=day_ids)
    # print(ob_data)
    if not ob_data.empty:
        ob_data['datetime'] = pd.to_datetime(
            ob_data.apply(lambda row: api_date_converter(dEven=row.dEven, hEven=int(row.hEven)), axis=1)
        )

        ob_data.rename({'number': 'Depth', 'zOrdMeOf': 'Sell_No', 'qTitMeOf': 'Sell_Vol',
                        'pMeOf': 'Sell_Price', 'zOrdMeDem': 'Buy_No', 'qTitMeDem': 'Buy_Vol',
                        'pMeDem': 'Buy_Price' }, axis='columns', inplace=True)

        ob_data.sort_values(by=['datetime'], ascending=True, inplace=True)
        ob_data = ob_data[['datetime', 'Depth', 'Sell_No', 'Sell_Vol', 'Sell_Price', 'Buy_No', 'Buy_Vol',
                           'Buy_Price']]
        return ob_data
    else:
        return pd.DataFrame()