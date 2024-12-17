import pandas as pd
from sqlalchemy import Column, Integer, NVARCHAR, Float, BigInteger, Date, DateTime, ForeignKey, BINARY, Boolean, text
from sqlalchemy.orm import relationship
from config.settings import BaseFinancialDB

class Company(BaseFinancialDB):
    __tablename__ = "company"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(NVARCHAR(50))
    name = Column(NVARCHAR)
    TickerEN = Column(NVARCHAR(50))
    NameEN = Column(NVARCHAR(150))
    Market = Column(NVARCHAR(50))
    InsCode = Column(BigInteger, nullable=False)
    Panel = Column(NVARCHAR(100))
    Sector = Column(NVARCHAR(100))
    SubSector = Column(NVARCHAR(150))
    CompanyCode = Column(NVARCHAR(30))
    TickerEn5char = Column(NVARCHAR(5))
    TickerEn12char = Column(NVARCHAR(12))
    Comments = Column(NVARCHAR)

    daily_prices = relationship("DailyPrice", back_populates="company")
    final_moments = relationship("FinalMoment", back_populates="company")
    moment_prices = relationship("MomentPrice", back_populates="company")
    ob_history = relationship("OBHistory", back_populates="company")

class DailyPrice(BaseFinancialDB):
    __tablename__ = "daily_price"

    id = Column(Integer, primary_key=True, autoincrement=True)
    CompanyID = Column(Integer, ForeignKey("company.id"), nullable=False)
    date = Column(Date, nullable=False)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    final = Column(Integer)
    volume = Column(BigInteger)
    value = Column(BigInteger)
    count = Column(Integer)
    jdate = Column(NVARCHAR(10))
    y_final = Column(Integer)

    company = relationship("Company", back_populates="daily_prices")

class DollarPrice(BaseFinancialDB):
    __tablename__ = "dollar_price"

    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    jdate = Column(NVARCHAR(10))

class FinalMoment(BaseFinancialDB):
    __tablename__ = "FinalMoment"

    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime)
    CompanyID = Column(Integer, ForeignKey("company.id"), nullable=False)
    Final = Column(Integer)
    close = Column(Integer)
    count = Column(Integer)
    volume = Column(BigInteger)
    MarketCap = Column(BigInteger)

    company = relationship("Company", back_populates="final_moments")

class Holiday(BaseFinancialDB):
    __tablename__ = "holiday"

    id = Column(BigInteger, primary_key=True)
    JDname = Column(NVARCHAR(20))
    EventName = Column(NVARCHAR(200))
    Holiday = Column(BINARY)
    Date = Column(Date)
    JDate = Column(NVARCHAR(10))

class IndiceData(BaseFinancialDB):
    __tablename__ = "indiceData"

    id = Column(Integer, primary_key=True, autoincrement=True)
    Datetime = Column(DateTime, nullable=False)
    JDate = Column(DateTime, nullable=False)
    Open = Column(Float, nullable=False)
    High = Column(Float, nullable=False)
    Low = Column(Float, nullable=False)
    Close = Column(Float, nullable=False)
    AdjClose = Column(Float)
    Volume = Column(BigInteger)
    IndiceID = Column(Integer, ForeignKey("IndicesType.id"), nullable=False)

class IndicesType(BaseFinancialDB):
    __tablename__ = "IndicesType"

    id = Column(Integer, primary_key=True, autoincrement=True)
    Name = Column(NVARCHAR(500), nullable=False)

class MomentPrice(BaseFinancialDB):
    __tablename__ = "MomentPrice"

    id = Column(Integer, primary_key=True, autoincrement=True)
    CompanyID = Column(Integer, ForeignKey("company.id"), nullable=False)
    Volume = Column(BigInteger)
    datetime = Column(DateTime, nullable=False)
    Price = Column(BigInteger)
    Count = Column(Integer)

    company = relationship("Company", back_populates="moment_prices")

class OBHistory(BaseFinancialDB):
    __tablename__ = "OBHistory"

    id = Column(Integer, primary_key=True, autoincrement=True)
    datetime = Column(DateTime)
    Depth = Column(Integer)
    Sell_No = Column(BigInteger)
    Sell_Vol = Column(BigInteger)
    Sell_Price = Column(Float)
    Buy_No = Column(BigInteger)
    Buy_Vol = Column(BigInteger)
    Buy_Price = Column(Float)
    CompanyID = Column(Integer, ForeignKey("company.id"), nullable=False)
    DateOnly = Column(Date)

    company = relationship("Company", back_populates="ob_history")

class OHLC_Summary(BaseFinancialDB):
    __tablename__ = "OHLC_Summary"

    CompanyID = Column(Integer, primary_key=True)
    TimeInterval = Column(DateTime, primary_key=True)
    IntervalLength = Column(Integer, primary_key=True)
    Open = Column(Integer)
    High = Column(Integer)
    Low = Column(Integer)
    Close = Column(Integer)
    TotalVolume = Column(BigInteger)
    TotalMarketCap = Column(BigInteger)
    @staticmethod
    def update_ohlc_summary(session):
        """
        Executes the UpdateOHLC_Summary stored procedure.

        Args:
            session (Session): SQLAlchemy session object.
        """
        try:
            session.execute(text("EXEC UpsertOHLC_Summary"))
            session.commit()
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Failed to execute PopulateDataStatus: {e}")


class DataStatus(BaseFinancialDB):
    __tablename__ = "data_status"

    date = Column(Date, primary_key=True, nullable=False)
    company_id = Column(Integer, ForeignKey("company.id"), primary_key=True, nullable=False)
    daily = Column(Boolean, nullable=False, default=False)
    moment = Column(Boolean, nullable=False, default=False)
    final = Column(Boolean, nullable=False, default=False)
    orderbook = Column(Boolean, nullable=False, default=False)
    ohlc = Column(Boolean, nullable=False, default=False)
    is_checked = Column(Boolean, nullable=False, default=False)

    @staticmethod
    def populate_data_status(session):
        """
        Executes the PopulateDataStatus stored procedure.

        Args:
            session (Session): SQLAlchemy session object.
        """
        try:
            session.execute(text("EXEC PopulateDataStatus"))
            session.commit()
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Failed to execute PopulateDataStatus: {e}")