from app.models.financial_db import MomentPrice , Company
from app.controllers.crud.crud_operations import get_by_condition , get_all
from datetime import datetime,timedelta
from config.settings import FinancialSessionLocal

db = FinancialSessionLocal()

companies = get_all(db , Company , as_dataframe= True)

start_of_day = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
end_of_day = start_of_day + timedelta(days=1)

condition = MomentPrice.datetime.between(start_of_day, end_of_day)
results = get_by_condition(db, MomentPrice, condition, as_dataframe=True)


print(results)
