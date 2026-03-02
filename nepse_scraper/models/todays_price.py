from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict


class SortInfo(BaseModel):
    sorted: bool
    unsorted: bool
    empty: bool


class Pageable(BaseModel):
    sort: SortInfo
    offset: int
    page_number: int = Field(alias="pageNumber")
    page_size: int = Field(alias="pageSize")
    paged: bool
    unpaged: bool


class StockData(BaseModel):
    id: Optional[int] = None
    business_date: date = Field(alias="businessDate")
    security_id: int = Field(alias="securityId")
    symbol: str
    security_name: str = Field(alias="securityName")
    open_price: float = Field(alias="openPrice")
    high_price: float = Field(alias="highPrice")
    low_price: float = Field(alias="lowPrice")
    close_price: float = Field(alias="closePrice")
    total_traded_quantity: int = Field(alias="totalTradedQuantity")
    total_traded_value: float = Field(alias="totalTradedValue")
    previous_day_close_price: float = Field(alias="previousDayClosePrice")
    fifty_two_week_high: float = Field(alias="fiftyTwoWeekHigh")
    fifty_two_week_low: float = Field(alias="fiftyTwoWeekLow")
    last_updated_time: datetime = Field(alias="lastUpdatedTime")
    last_updated_price: float = Field(alias="lastUpdatedPrice")
    total_trades: int = Field(alias="totalTrades")
    average_traded_price: float = Field(alias="averageTradedPrice")
    market_capitalization: float = Field(alias="marketCapitalization")

    model_config = ConfigDict(populate_by_name=True)


class TodaysPriceResponse(BaseModel):
    content: List[StockData]
    pageable: Pageable
    last: bool
    total_pages: int = Field(alias="totalPages")
    total_elements: int = Field(alias="totalElements")
    size: int
    number: int
    sort: SortInfo
    first: bool
    number_of_elements: int = Field(alias="numberOfElements")
    empty: bool

    model_config = ConfigDict(populate_by_name=True)
