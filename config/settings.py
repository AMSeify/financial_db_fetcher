import os

from bleach.six_shim import urllib
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv
import urllib


# Load environment variables
load_dotenv()

# SQLAlchemy base for both databases
BaseFinancialDB = declarative_base()
BaseCodalDB = declarative_base()

# FinancialDB configurations
financial_db_url = (
    f"mssql+pyodbc://{os.getenv('FINANCIAL_DB_USER')}:{urllib.parse.quote(os.getenv('FINANCIAL_DB_PASSWORD'))}"
    f"@{os.getenv('FINANCIAL_DB_SERVER')}/{os.getenv('FINANCIAL_DB_DATABASE')}"
    f"?driver={os.getenv('FINANCIAL_DB_DRIVER')}"
    f"&TrustServerCertificate={os.getenv('FINANCIAL_DB_TRUST_CERTIFICATE')}"
)

# CodalDB configurations
codal_db_url = (
    f"mssql+pyodbc://{os.getenv('CODAL_DB_USER')}:{urllib.parse.quote(os.getenv('CODAL_DB_PASSWORD'))}"
    f"@{os.getenv('CODAL_DB_SERVER')}/{os.getenv('CODAL_DB_DATABASE')}"
    f"?driver={os.getenv('CODAL_DB_DRIVER')}"
    f"&TrustServerCertificate={os.getenv('CODAL_DB_TRUST_CERTIFICATE')}"
)


# Create engines
financial_engine = create_engine(financial_db_url)
codal_engine = create_engine(codal_db_url)

# Create session factories
FinancialSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=financial_engine)
CodalSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=codal_engine)


# db = FinancialSessionLocal()
# try:
#     db.execute(text("SELECT @@VERSION"))
#     print("Connection successful!")
# finally:
#     db.close()


# db = CodalSessionLocal()
# try:
#     db.execute(text("SELECT @@VERSION"))
#     print("Connection successful!")
# finally:
#     db.close()
