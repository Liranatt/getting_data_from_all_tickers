import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class StockDataInserter:
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize with database configuration

        Args:
            db_config: Dict with keys: host, database, user, password, port
        """
        self.db_config = db_config

    def get_db_connection(self):
        """Create and return a database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def _get_table_columns(self, table_name: str, cursor) -> List[str]:
        """Helper to get column names for a given table."""
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = '{table_name}';
        """)
        return [row[0] for row in cursor.fetchall()]

    # ... (no changes to insert_price_data, insert_org_overview, insert_calendar_events) ...
    def insert_price_data(self, ticker: str, data: Optional[Dict]) -> bool:
        if not data: return False
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
                INSERT INTO price (symbol, regularMarketPrice, regularMarketChangePercent, regularMarketChange,
                    regularMarketDayHigh, regularMarketDayLow, regularMarketVolume, regularMarketPreviousClose, 
                    regularMarketOpen, exchangeName, longName, currency, marketCap) 
                VALUES (%(symbol)s, %(regularMarketPrice)s, %(regularMarketChangePercent)s, %(regularMarketChange)s,
                    %(regularMarketDayHigh)s, %(regularMarketDayLow)s, %(regularMarketVolume)s, %(regularMarketPreviousClose)s,
                    %(regularMarketOpen)s, %(exchangeName)s, %(longName)s, %(currency)s, %(marketCap)s)
                ON CONFLICT (symbol, regularMarketTime) DO NOTHING;"""  # Added ON CONFLICT
            cursor.execute(insert_query, data)
            conn.commit()
            logger.info(f"Inserted price data for {ticker}")
            return True
        except Exception as e:
            logger.error(f"Error inserting price data for {ticker}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def insert_org_overview(self, ticker: str, data: Optional[Dict]) -> bool:
        if not data: return False
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
                INSERT INTO org_overview (symbol, address1, city, zip, country, phone, website, industry,
                    sector, longBusinessSummary, fullTimeEmployees, enterpriseValue, bookValue, 
                    priceToBook, forwardPE, profitMargins, sharesOutstanding) 
                VALUES (%(symbol)s, %(address1)s, %(city)s, %(zip)s, %(country)s, %(phone)s, %(website)s,
                    %(industry)s, %(sector)s, %(longBusinessSummary)s, %(fullTimeEmployees)s, %(enterpriseValue)s,
                    %(bookValue)s, %(priceToBook)s, %(forwardPE)s, %(profitMargins)s, %(sharesOutstanding)s)
                ON CONFLICT (symbol) DO UPDATE SET
                    address1 = EXCLUDED.address1, city = EXCLUDED.city, zip = EXCLUDED.zip, country = EXCLUDED.country,
                    phone = EXCLUDED.phone, website = EXCLUDED.website, industry = EXCLUDED.industry, sector = EXCLUDED.sector,
                    longBusinessSummary = EXCLUDED.longBusinessSummary, fullTimeEmployees = EXCLUDED.fullTimeEmployees,
                    enterpriseValue = EXCLUDED.enterpriseValue, bookValue = EXCLUDED.bookValue, priceToBook = EXCLUDED.priceToBook,
                    forwardPE = EXCLUDED.forwardPE, profitMargins = EXCLUDED.profitMargins, sharesOutstanding = EXCLUDED.sharesOutstanding,
                    created_at = CURRENT_TIMESTAMP;"""
            cursor.execute(insert_query, data)
            conn.commit()
            logger.info(f"Inserted/Updated org overview for {ticker}")
            return True
        except Exception as e:
            logger.error(f"Error inserting org overview for {ticker}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def insert_calendar_events(self, ticker: str, data: Optional[Dict]) -> bool:
        if not data or data.get('earnings_date') is None: return False
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
                INSERT INTO calendar_events (symbol, earnings_date, earnings_average, earnings_low, earnings_high, revenue_average)
                VALUES (%(symbol)s, %(earnings_date)s, %(earnings_average)s, %(earnings_low)s, %(earnings_high)s, %(revenue_average)s)
                ON CONFLICT (symbol, earnings_date) DO UPDATE SET
                    earnings_average = EXCLUDED.earnings_average, earnings_low = EXCLUDED.earnings_low,
                    earnings_high = EXCLUDED.earnings_high, revenue_average = EXCLUDED.revenue_average,
                    created_at = CURRENT_TIMESTAMP;"""
            cursor.execute(insert_query, data)
            conn.commit()
            logger.info(f"Inserted/Updated calendar events for {ticker}")
            return True
        except Exception as e:
            logger.error(f"Error inserting calendar events for {ticker}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def insert_dataframe_to_table(self, df: Optional[pd.DataFrame], table_name: str, ticker: str,
                                  conflict_key: List[str]) -> bool:
        """Generic method to insert/update a DataFrame into any table."""
        if df is None or df.empty:
            logger.warning(f"No {table_name} data to insert for {ticker}")
            return False

        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            table_columns = self._get_table_columns(table_name, cursor)
            df_columns = [col for col in df.columns if col in table_columns]
            df_clean = df[df_columns].copy()

            if df_clean.empty:
                logger.warning(f"No matching columns for {table_name} table for {ticker}")
                return False

            values = [tuple(row) for row in df_clean.to_records(index=False)]

            columns_str = ','.join(df_columns)

            # Create ON CONFLICT clause
            update_columns = [col for col in df_columns if col not in conflict_key]
            update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            conflict_str = ", ".join(conflict_key)

            insert_query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES %s
                ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}, created_at = CURRENT_TIMESTAMP;
            """

            execute_values(cursor, insert_query, values)
            conn.commit()

            logger.info(f"Inserted/Updated {len(values)} rows into {table_name} for {ticker}")
            return True
        except Exception as e:
            logger.error(f"Error inserting {table_name} data for {ticker}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def insert_announcement_price(self, ticker: str, df: Optional[pd.DataFrame]) -> bool:
        """
        NEW: Insert announcement price data.
        Conflict on symbol and announcement_date ensures we don't duplicate records.
        """
        return self.insert_dataframe_to_table(df, 'announcement_price', ticker, ['symbol', 'announcement_date'])

    def insert_balance_sheet(self, ticker: str, df: Optional[pd.DataFrame]) -> bool:
        """Insert balance sheet data"""
        return self.insert_dataframe_to_table(df, 'balance_sheet', ticker, ['symbol', 'asOfDate'])

    def insert_income_statement(self, ticker: str, df: Optional[pd.DataFrame]) -> bool:
        """Insert income statement data"""
        return self.insert_dataframe_to_table(df, 'income_statement', ticker, ['symbol', 'asOfDate'])

    def insert_cash_flow(self, ticker: str, df: Optional[pd.DataFrame]) -> bool:
        """Insert cash flow data"""
        return self.insert_dataframe_to_table(df, 'cash_flow', ticker, ['symbol', 'asOfDate'])

    def insert_valuation_measures(self, ticker: str, data: Optional[Dict]) -> bool:
        # ... (no changes to this method, but an ON CONFLICT could be added) ...
        if not data: return False
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            insert_query = """
                INSERT INTO valuation_measures (symbol, PriceToEarningsRatio, PriceToSalesRatio, PriceToBookRatio,
                    EnterpiseToRevenue, EnterpiseToEbitda, asOfDate) 
                VALUES (%(symbol)s, %(PriceToEarningsRatio)s, %(PriceToSalesRatio)s, %(PriceToBookRatio)s,
                    %(EnterpiseToRevenue)s, %(EnterpiseToEbitda)s, %(asOfDate)s)
                ON CONFLICT (symbol, asOfDate) DO NOTHING;"""
            data['asOfDate'] = datetime.now().date()
            cursor.execute(insert_query, data)
            conn.commit()
            logger.info(f"Inserted valuation measures for {ticker}")
            return True
        except Exception as e:
            logger.error(f"Error inserting valuation measures for {ticker}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def insert_institution_ownership(self, ticker: str, df: Optional[pd.DataFrame]) -> bool:
        """Insert institutional ownership data"""
        # We might need to delete old records before inserting new ones
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM institution_ownership WHERE symbol = %s", (ticker,))
            conn.commit()
        except Exception as e:
            logger.error(f"Could not clear old institutional holders for {ticker}: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
        return self.insert_dataframe_to_table(df, 'institution_ownership', ticker, ['symbol', 'organization'])

    def insert_executive_ownership(self, ticker: str, df: Optional[pd.DataFrame]) -> bool:
        """Insert executive ownership data"""
        return self.insert_dataframe_to_table(df, 'executive_ownership', ticker, ['symbol', 'name', 'latestTransDate'])

    def calculate_and_insert_quarterly_price(self, ticker: str) -> bool:
        # ... (no significant changes, but ON CONFLICT added for robustness) ...
        try:
            import yfinance as yf
            stock = yf.Ticker(ticker)
            hist_data = stock.history(period="2y")
            if hist_data.empty: return False

            hist_data.index = pd.to_datetime(hist_data.index.date)
            quarterly_data = hist_data.resample('Q').agg(Close=('Close', 'mean'), High=('High', 'max'),
                                                         Low=('Low', 'min'), Volume=('Volume', 'mean')).dropna()
            if quarterly_data.empty: return False

            quarterly_records = [
                {'symbol': ticker, 'quarter': f"Q{q.quarter}", 'year': q.year, 'avg_price': float(r['Close']),
                 'high_price': float(r['High']), 'low_price': float(r['Low']), 'volume_avg': int(r['Volume'])}
                for q, r in quarterly_data.iterrows()]

            conn = self.get_db_connection()
            cursor = conn.cursor()
            try:
                insert_query = """
                    INSERT INTO quarterly_price (symbol, quarter, year, avg_price, high_price, low_price, volume_avg)
                    VALUES (%(symbol)s, %(quarter)s, %(year)s, %(avg_price)s, %(high_price)s, %(low_price)s, %(volume_avg)s)
                    ON CONFLICT (symbol, quarter, year) DO UPDATE SET
                        avg_price = EXCLUDED.avg_price, high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price, volume_avg = EXCLUDED.volume_avg,
                        created_at = CURRENT_TIMESTAMP;"""
                cursor.executemany(insert_query, quarterly_records)
                conn.commit()
                logger.info(f"Inserted/Updated {len(quarterly_records)} quarterly price records for {ticker}")
                return True
            except Exception as e:
                logger.error(f"Error inserting quarterly price data for {ticker}: {e}")
                conn.rollback()
                return False
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            logger.error(f"Failed during quarterly price calculation for {ticker}: {e}")
            return False

    def insert_all_data(self, ticker: str, data: Dict[str, Any]):
        """Main method to insert all extracted data for a ticker."""
        logger.info(f"Starting data insertion process for {ticker}")

        self.insert_price_data(ticker, data.get('price'))
        self.insert_org_overview(ticker, data.get('org_overview'))
        self.insert_calendar_events(ticker, data.get('calendar_events'))
        self.insert_balance_sheet(ticker, data.get('balance_sheet'))
        self.insert_income_statement(ticker, data.get('income_statement'))
        self.insert_cash_flow(ticker, data.get('cash_flow'))
        self.insert_valuation_measures(ticker, data.get('valuation_measures'))
        self.insert_institution_ownership(ticker, data.get('institution_ownership'))
        self.insert_executive_ownership(ticker, data.get('executive_ownership'))

        # NEW: Insert announcement price data
        self.insert_announcement_price(ticker, data.get('announcement_price'))

        # Perform and insert derived calculations
        self.calculate_and_insert_quarterly_price(ticker)

        logger.info(f"Finished data insertion process for {ticker}")