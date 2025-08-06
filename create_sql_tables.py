import psycopg2
import logging

logger = logging.getLogger(__name__)


class StockTableCreator:
    def __init__(self, db_config):
        """
        Initialize with database configuration
        
        Args:
            db_config: Dict with keys: host, database, user, password, port
        """
        self.db_config = db_config
    
    def create_all_tables(self):
        """Create all stock-related tables in PostgreSQL"""
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        
        try:
            # Price table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS price (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    regularMarketPrice DECIMAL,
                    regularMarketChangePercent DECIMAL,
                    regularMarketChange DECIMAL,
                    regularMarketTime TIMESTAMP,
                    regularMarketDayHigh DECIMAL,
                    regularMarketDayLow DECIMAL,
                    regularMarketVolume BIGINT,
                    regularMarketPreviousClose DECIMAL,
                    regularMarketSource VARCHAR(100),
                    regularMarketOpen DECIMAL,
                    exchangeName VARCHAR(100),
                    marketState VARCHAR(50),
                    quoteType VARCHAR(50),
                    longName TEXT,
                    currency VARCHAR(10),
                    quoteSourceName VARCHAR(100),
                    currencySymbol VARCHAR(10),
                    marketCap BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Quarterly price table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quarterly_price (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    quarter VARCHAR(10),
                    year INTEGER,
                    avg_price DECIMAL,
                    high_price DECIMAL,
                    low_price DECIMAL,
                    volume_avg BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Announcement price table (before/after earnings)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS announcement_price (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    announcement_date DATE,
                    announcement_type VARCHAR(50),
                    price_before DECIMAL,
                    price_after DECIMAL,
                    change_percent DECIMAL,
                    days_before INTEGER DEFAULT 1,
                    days_after INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Balance sheet table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS balance_sheet (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    asOfDate DATE,
                    currencyCode VARCHAR(10),
                    AccountsPayable BIGINT,
                    AccountsReceivable BIGINT,
                    CashAndCashEquivalents BIGINT,
                    CommonStockEquity BIGINT,
                    CurrentAssets BIGINT,
                    CurrentLiabilities BIGINT,
                    Goodwill BIGINT,
                    GrossPPE BIGINT,
                    Inventory BIGINT,
                    LongTermDebt BIGINT,
                    NetDebt BIGINT,
                    NetPPE BIGINT,
                    NetTangibleAssets BIGINT,
                    Receivables BIGINT,
                    RetainedEarnings BIGINT,
                    TotalAssets BIGINT,
                    TotalEquityGrossMinorityInterest BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Income statement table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS income_statement (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    asOfDate DATE,
                    currencyCode VARCHAR(10),
                    TotalRevenue BIGINT,
                    CostOfRevenue BIGINT,
                    GrossProfit BIGINT,
                    OperatingExpense BIGINT,
                    OperatingIncome BIGINT,
                    InterestExpense BIGINT,
                    PretaxIncome BIGINT,
                    TaxProvision BIGINT,
                    NetIncome BIGINT,
                    BasicEPS DECIMAL,
                    DilutedEPS DECIMAL,
                    EBIT BIGINT,
                    EBITDA BIGINT,
                    OperatingRevenue BIGINT,
                    SellingGeneralAndAdministration BIGINT,
                    ResearchAndDevelopment BIGINT,
                    NetIncomeFromContinuingOperationNetMinorityInterest BIGINT,
                    NetIncomeCommonStockholders BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Cash flow table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cash_flow (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    asOfDate DATE,
                    currencyCode VARCHAR(10),
                    OperatingCashFlow BIGINT,
                    InvestingCashFlow BIGINT,
                    FinancingCashFlow BIGINT,
                    NetIncome BIGINT,
                    DepreciationAndAmortization BIGINT,
                    ChangeInWorkingCapital BIGINT,
                    CapitalExpenditure BIGINT,
                    NetBusinessPurchaseAndSale BIGINT,
                    NetInvestmentPurchaseAndSale BIGINT,
                    NetPPEPurchaseAndSale BIGINT,
                    IssuanceOfDebt BIGINT,
                    RepaymentOfDebt BIGINT,
                    CommonStockIssuance BIGINT,
                    CommonStockPayments BIGINT,
                    FreeCashFlow BIGINT,
                    BeginningCashPosition BIGINT,
                    EndCashPosition BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Organization overview table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS org_overview (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    address1 TEXT,
                    city VARCHAR(100),
                    zip VARCHAR(20),
                    country VARCHAR(100),
                    phone VARCHAR(50),
                    website TEXT,
                    industry VARCHAR(200),
                    industryDisp VARCHAR(200),
                    sector VARCHAR(200),
                    longBusinessSummary TEXT,
                    fullTimeEmployees INTEGER,
                    priceHint INTEGER,
                    enterpriseValue BIGINT,
                    bookValue DECIMAL,
                    priceToBook DECIMAL,
                    enterpriseToRevenue DECIMAL,
                    forwardPE DECIMAL,
                    enterpriseToEbitda DECIMAL,
                    fiftyTwoWeekChange DECIMAL,
                    profitMargins DECIMAL,
                    floatShares BIGINT,
                    sharesOutstanding BIGINT,
                    sharesShort BIGINT,
                    shortRatio DECIMAL,
                    sharesShortPriorMonth BIGINT,
                    heldPercentInstitutions DECIMAL,
                    heldPercentInsiders DECIMAL,
                    trailingEps DECIMAL,
                    forwardEps DECIMAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Calendar events table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS calendar_events (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    earnings_date DATE,
                    earnings_average DECIMAL,
                    earnings_low DECIMAL,
                    earnings_high DECIMAL,
                    revenue_average BIGINT,
                    revenue_low BIGINT,
                    revenue_high BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Executive payment packages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS exec_payment_packages (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    name VARCHAR(200),
                    title VARCHAR(200),
                    totalPay BIGINT,
                    exercisedValue BIGINT,
                    unexercisedValue BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Institution ownership table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS institution_ownership (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    organization VARCHAR(200),
                    pctHeld DECIMAL,
                    position BIGINT,
                    value BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Executive ownership table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS executive_ownership (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    name VARCHAR(200),
                    relation VARCHAR(200),
                    url TEXT,
                    transactionDescription VARCHAR(500),
                    latestTransDate DATE,
                    positionDirect BIGINT,
                    positionDirectDate DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Valuation measures table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS valuation_measures (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20),
                    asOfDate DATE,
                    PriceToEarningsRatio DECIMAL,
                    PriceToSalesRatio DECIMAL,
                    PriceToBookRatio DECIMAL,
                    EnterpiseToRevenue DECIMAL,
                    EnterpiseToEbitda DECIMAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            logger.info("All tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()


# Usage example:
# db_config = {
#     'host': 'localhost',
#     'database': 'stocks',
#     'user': 'your_user',
#     'password': 'your_password',
#     'port': 5432
# }
# 
# # In your loop:
# for i in range(big_number):
#     table_creator = StockTableCreator(db_config)
#     table_creator.create_all_tables()
#     # ... your data filling logic here