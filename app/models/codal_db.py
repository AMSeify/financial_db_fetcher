from sqlalchemy import Column, Integer, NVARCHAR, BigInteger, ForeignKey
from sqlalchemy.orm import relationship
from config.settings import BaseCodalDB

class Categories(BaseCodalDB):
    __tablename__ = "Categories"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(Integer, unique=True)
    name = Column(NVARCHAR(100))

class Companies(BaseCodalDB):
    __tablename__ = "Companies"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    short_name = Column(NVARCHAR(100))
    full_name = Column(NVARCHAR(100))
    company_id = Column(BigInteger, unique=True)
    type = Column(Integer)
    status = Column(Integer)
    industry_group = Column(Integer)
    rating_type = Column(Integer)

    letters = relationship("Letters", back_populates="company")
    profit_loss_statements = relationship("ProfitLossStatement", back_populates="company")

class FinancialYears(BaseCodalDB):
    __tablename__ = "FinancialYears"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    year_date = Column(NVARCHAR(10), unique=True)

class IndustryGroup(BaseCodalDB):
    __tablename__ = "IndustryGroup"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(NVARCHAR(100))
    ind_id = Column(Integer, unique=True)

class Letters(BaseCodalDB):
    __tablename__ = "Letters"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tracing_no = Column(Integer, unique=True)
    letter_type_id = Column(Integer, ForeignKey("LetterTypes.letter_type_id"))
    company_id = Column(BigInteger, ForeignKey("Companies.company_id"))
    symbol = Column(NVARCHAR(1000))
    company_name = Column(NVARCHAR(1000))
    title = Column(NVARCHAR(1000))
    letter_code = Column(NVARCHAR(1000))
    sent_date_time = Column(NVARCHAR(100))
    publish_date_time = Column(NVARCHAR(100))
    url = Column(NVARCHAR(1000))
    pdf_url = Column(NVARCHAR(1000))
    excel_url = Column(NVARCHAR(1000))

    company = relationship("Companies", back_populates="letters")

class LetterTypes(BaseCodalDB):
    __tablename__ = "LetterTypes"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(NVARCHAR(1000))
    category_code = Column(Integer, ForeignKey("Categories.code"))
    letter_type_id = Column(Integer, unique=True)

class ProfitLossStatement(BaseCodalDB):
    __tablename__ = "proft_loss_statement"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(NVARCHAR(1000))
    Fin_Years = Column(NVARCHAR(10), nullable=False)
    data = Column(BigInteger)
    Period_Month = Column(Integer)
    is_subCo = Column(Integer)
    renew = Column(Integer)
    accounted = Column(Integer)
    company_Id = Column(BigInteger, ForeignKey("Companies.company_id"))
    letter_id = Column(Integer, ForeignKey("Letters.id"))

    company = relationship("Companies", back_populates="profit_loss_statements")
