import pandas as pd
import logging
from typing import Dict, List, Optional, Any
import yfinance as yf
from pandas.tseries.offsets import BDay  # Used for business day calculations

logger = logging.getLogger(__name__)


class StockDataExtractor:
    def __init__(self, fmp_client=None, finnhub_client=None, alpha_vantage_client=None):
        """
        Initialize with API clients (assuming they're already configured)

        Args:
            fmp_client: Financial Modeling Prep client
            finnhub_client: Finnhub client
            alpha_vantage_client: Alpha Vantage client
        """
        self.fmp_client = fmp_client
        self.finnhub_client = finnhub_client
        self.alpha_vantage_client = alpha_vantage_client

        # Define API priority order for each data type
        self.api_priority = {
            'price': ['yfinance', 'alpha_vantage'],
            'org_overview': ['yfinance', 'fmp', 'finnhub'],
            'calendar_events': ['yfinance', 'fmp', 'alpha_vantage'],
            'balance_sheet': ['yfinance', 'fmp', 'alpha_vantage'],
            'cash_flow': ['yfinance', 'fmp', 'alpha_vantage'],
            'income_statement': ['yfinance', 'fmp', 'alpha_vantage'],
            'valuation_measures': ['yfinance', 'fmp'],
            'institution_ownership': ['yfinance', 'fmp'],
            'executive_ownership': ['finnhub', 'yfinance'],
            'announcement_price': ['yfinance'],  # New entry
            'exec_payment_packages': []
        }

    def extract_price_data(self, ticker: str) -> Optional[Dict]:
        """Extract price data with fallback logic"""
        for api in self.api_priority['price']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    if info.get('regularMarketPrice') is not None:
                        return {
                            'symbol': ticker,
                            'regularMarketPrice': info.get('regularMarketPrice'),
                            'regularMarketChangePercent': info.get('regularMarketChangePercent'),
                            'regularMarketChange': info.get('regularMarketChange'),
                            'regularMarketDayHigh': info.get('regularMarketDayHigh'),
                            'regularMarketDayLow': info.get('regularMarketDayLow'),
                            'regularMarketVolume': info.get('regularMarketVolume'),
                            'regularMarketPreviousClose': info.get('regularMarketPreviousClose'),
                            'regularMarketOpen': info.get('regularMarketOpen'),
                            'exchangeName': info.get('exchange'),
                            'longName': info.get('longName'),
                            'currency': info.get('currency'),
                            'marketCap': info.get('marketCap')
                        }

                elif api == 'alpha_vantage' and self.alpha_vantage_client:
                    data = self.alpha_vantage_client.get_quote(ticker)
                    if data:
                        return self._format_alpha_vantage_price(data, ticker)

            except Exception as e:
                logger.warning(f"Failed to get price data from {api} for {ticker}: {e}")
                continue

        logger.error(f"Could not retrieve price data for {ticker} from any source")
        return None

    def extract_org_overview(self, ticker: str) -> Optional[Dict]:
        """Extract organization overview with fallback logic"""
        for api in self.api_priority['org_overview']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    if info:
                        return {
                            'symbol': ticker,
                            'address1': info.get('address1'),
                            'city': info.get('city'),
                            'zip': info.get('zip'),
                            'country': info.get('country'),
                            'phone': info.get('phone'),
                            'website': info.get('website'),
                            'industry': info.get('industry'),
                            'sector': info.get('sector'),
                            'longBusinessSummary': info.get('longBusinessSummary'),
                            'fullTimeEmployees': info.get('fullTimeEmployees'),
                            'enterpriseValue': info.get('enterpriseValue'),
                            'bookValue': info.get('bookValue'),
                            'priceToBook': info.get('priceToBook'),
                            'forwardPE': info.get('forwardPE'),
                            'profitMargins': info.get('profitMargins'),
                            'sharesOutstanding': info.get('sharesOutstanding')
                        }

                elif api == 'fmp' and self.fmp_client:
                    data = self.fmp_client.get_company_profile(ticker)
                    if data:
                        return self._format_fmp_overview(data, ticker)

                elif api == 'finnhub' and self.finnhub_client:
                    data = self.finnhub_client.get_company_profile(ticker)
                    if data:
                        return self._format_finnhub_overview(data, ticker)

            except Exception as e:
                logger.warning(f"Failed to get org overview from {api} for {ticker}: {e}")
                continue

        logger.error(f"Could not retrieve org overview for {ticker} from any source")
        return None

    def extract_calendar_events(self, ticker: str) -> Optional[Dict]:
        """Extract calendar events with fallback logic"""
        for api in self.api_priority['calendar_events']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    calendar = stock.calendar
                    if calendar is not None and not calendar.empty:
                        data = calendar.T.to_dict().get(0, {})
                        return {
                            'symbol': ticker,
                            'earnings_date': data.get('Earnings Date'),
                            'earnings_average': data.get('Earnings Average'),
                            'earnings_low': data.get('Earnings Low'),
                            'earnings_high': data.get('Earnings High'),
                            'revenue_average': data.get('Revenue Average'),
                        }

                elif api == 'fmp' and self.fmp_client:
                    data = self.fmp_client.get_earnings_calendar(ticker)
                    if data:
                        return self._format_fmp_calendar(data, ticker)

                elif api == 'alpha_vantage' and self.alpha_vantage_client:
                    data = self.alpha_vantage_client.get_earnings_calendar(ticker)
                    if data:
                        return self._format_alpha_vantage_calendar(data, ticker)

            except Exception as e:
                logger.warning(f"Failed to get calendar events from {api} for {ticker}: {e}")
                continue

        logger.error(f"Could not retrieve calendar events for {ticker} from any source")
        return None

    def extract_announcement_price(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        NEW: Extracts closing price before and opening price after an earnings announcement.
        """
        api = 'yfinance'
        try:
            stock = yf.Ticker(ticker)
            earnings_dates = stock.earnings_dates
            if earnings_dates is None or earnings_dates.empty:
                logger.warning(f"No earnings dates found for {ticker}")
                return None

            announcement_prices = []
            for announcement_date in earnings_dates.index:
                announcement_date = announcement_date.normalize()  # Remove time component

                # Define the date range for fetching data
                start_date = announcement_date - BDay(5)
                end_date = announcement_date + BDay(5)

                hist_data = yf.download(ticker, start=start_date, end=end_date, progress=False)

                if hist_data.empty:
                    continue

                # Get the trading day before the announcement
                day_before = announcement_date - BDay(1)

                # Get the trading day after the announcement
                day_after = announcement_date + BDay(1)

                price_before = hist_data.loc[hist_data.index.normalize() == day_before]['Close'].iloc[0] if not \
                hist_data.loc[hist_data.index.normalize() == day_before].empty else None
                price_after = hist_data.loc[hist_data.index.normalize() == day_after]['Open'].iloc[0] if not \
                hist_data.loc[hist_data.index.normalize() == day_after].empty else None

                if price_before is not None and price_after is not None:
                    change_percent = ((price_after - price_before) / price_before) * 100
                    announcement_prices.append({
                        'symbol': ticker,
                        'announcement_date': announcement_date.date(),
                        'announcement_type': 'Earnings',
                        'price_before': price_before,
                        'price_after': price_after,
                        'change_percent': change_percent
                    })

            if not announcement_prices:
                return None

            return pd.DataFrame(announcement_prices)

        except Exception as e:
            logger.error(f"Failed to get announcement prices from {api} for {ticker}: {e}")
            return None

    def extract_balance_sheet(self, ticker: str, quarterly: bool = False) -> Optional[pd.DataFrame]:
        # ... (no changes to this method) ...
        for api in self.api_priority['balance_sheet']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    bs = stock.quarterly_balance_sheet if quarterly else stock.balance_sheet
                    if bs is not None and not bs.empty:
                        bs_df = bs.T.reset_index()
                        bs_df['symbol'] = ticker
                        bs_df = bs_df.rename(columns={'index': 'asOfDate', 'asOfDate': 'asOfDate'})
                        return bs_df
                # ... other APIs
            except Exception as e:
                logger.warning(f"Failed to get balance sheet from {api} for {ticker}: {e}")
                continue
        return None

    def extract_income_statement(self, ticker: str, quarterly: bool = False) -> Optional[pd.DataFrame]:
        # ... (no changes to this method) ...
        for api in self.api_priority['income_statement']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    is_df = stock.quarterly_financials if quarterly else stock.financials
                    if is_df is not None and not is_df.empty:
                        is_df = is_df.T.reset_index()
                        is_df['symbol'] = ticker
                        is_df = is_df.rename(columns={'index': 'asOfDate'})
                        return is_df
                # ... other APIs
            except Exception as e:
                logger.warning(f"Failed to get income statement from {api} for {ticker}: {e}")
                continue
        return None

    def extract_cash_flow(self, ticker: str, quarterly: bool = False) -> Optional[pd.DataFrame]:
        # ... (no changes to this method) ...
        for api in self.api_priority['cash_flow']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    cf = stock.quarterly_cashflow if quarterly else stock.cashflow
                    if cf is not None and not cf.empty:
                        cf_df = cf.T.reset_index()
                        cf_df['symbol'] = ticker
                        cf_df = cf_df.rename(columns={'index': 'asOfDate'})
                        return cf_df
                # ... other APIs
            except Exception as e:
                logger.warning(f"Failed to get cash flow from {api} for {ticker}: {e}")
                continue
        return None

    def extract_valuation_measures(self, ticker: str) -> Optional[Dict]:
        # ... (no changes to this method) ...
        for api in self.api_priority['valuation_measures']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    if info:
                        return {
                            'symbol': ticker,
                            'PriceToEarningsRatio': info.get('trailingPE'),  # Using trailingPE for actual data
                            'PriceToSalesRatio': info.get('priceToSalesTrailing12Months'),
                            'PriceToBookRatio': info.get('priceToBook'),
                            'EnterpiseToRevenue': info.get('enterpriseToRevenue'),
                            'EnterpiseToEbitda': info.get('enterpriseToEbitda')
                        }
                # ... other APIs
            except Exception as e:
                logger.warning(f"Failed to get valuation from {api} for {ticker}: {e}")
                continue
        return None

    def extract_institution_ownership(self, ticker: str) -> Optional[pd.DataFrame]:
        # ... (no changes to this method) ...
        for api in self.api_priority['institution_ownership']:
            try:
                if api == 'yfinance':
                    stock = yf.Ticker(ticker)
                    institutional = stock.institutional_holders
                    if institutional is not None and not institutional.empty:
                        institutional['symbol'] = ticker
                        return institutional.rename(
                            columns={'Holder': 'organization', '% Out': 'pctHeld', 'Shares': 'position',
                                     'Value': 'value'})
                # ... other APIs
            except Exception as e:
                logger.warning(f"Failed to get institutional ownership from {api} for {ticker}: {e}")
                continue
        return None

    def extract_executive_ownership(self, ticker: str) -> Optional[pd.DataFrame]:
        # ... (no changes to this method) ...
        for api in self.api_priority['executive_ownership']:
            try:
                if api == 'finnhub' and self.finnhub_client:
                    data = self.finnhub_client.get_insider_transactions(ticker)
                    if data:
                        return self._format_finnhub_insider(data, ticker)
                # ... other APIs
            except Exception as e:
                logger.warning(f"Failed to get executive ownership from {api} for {ticker}: {e}")
                continue
        return None

    def extract_all_data(self, ticker: str, quarterly: bool = False) -> Dict[str, Any]:
        """Extract all available data for a ticker"""
        results = {}

        data_types = [
            'price', 'org_overview', 'calendar_events', 'balance_sheet',
            'income_statement', 'cash_flow', 'valuation_measures',
            'institution_ownership', 'executive_ownership',
            'announcement_price'  # New data type
        ]

        for data_type in data_types:
            logger.info(f"Extracting {data_type} for {ticker}")

            if data_type == 'price':
                results[data_type] = self.extract_price_data(ticker)
            elif data_type == 'org_overview':
                results[data_type] = self.extract_org_overview(ticker)
            elif data_type == 'calendar_events':
                results[data_type] = self.extract_calendar_events(ticker)
            elif data_type == 'balance_sheet':
                results[data_type] = self.extract_balance_sheet(ticker, quarterly)
            elif data_type == 'income_statement':
                results[data_type] = self.extract_income_statement(ticker, quarterly)
            elif data_type == 'cash_flow':
                results[data_type] = self.extract_cash_flow(ticker, quarterly)
            elif data_type == 'valuation_measures':
                results[data_type] = self.extract_valuation_measures(ticker)
            elif data_type == 'institution_ownership':
                results[data_type] = self.extract_institution_ownership(ticker)
            elif data_type == 'executive_ownership':
                results[data_type] = self.extract_executive_ownership(ticker)
            elif data_type == 'announcement_price':
                results[data_type] = self.extract_announcement_price(ticker)

        return results

    # Helper methods to format data from different APIs (stubs)
    def _format_fmp_overview(self, data, ticker):
        pass

    def _format_finnhub_overview(self, data, ticker):
        pass

    def _format_alpha_vantage_price(self, data, ticker):
        pass

    def _format_fmp_calendar(self, data, ticker):
        pass
    # ... etc.