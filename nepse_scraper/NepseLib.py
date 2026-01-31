# nepse/NepseLib.py

import json
import pathlib
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import httpx
import tqdm

from nepse_scraper.DummyIDUtils import DummyIDManager
from nepse_scraper.TokenUtils import TokenManager
from nepse_scraper.Errors import (
    NepseInvalidClientRequest,
    NepseInvalidServerResponse,
    NepseNetworkError,
    NepseTokenExpired,
)


def _sanitize_headers(headers):
    """Remove sensitive tokens from metadata logs"""
    if not headers:
        return {}
    safe = dict(headers)
    if "Authorization" in safe:
        safe["Authorization"] = f"Salter {safe['Authorization'][:10]}...<redacted>"
    return safe


def _create_meta_skeleton(method, url, headers, payload=None):
    """Initialize metadata structure for every request"""
    return {
        "source": "nepalstock",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
        "http_status": None,
        "request_id": str(uuid.uuid4()),
        "response_time_ms": 0,
        "retry_count": 0,
        "request": {
            "method": method,
            "url": url,
            "headers": _sanitize_headers(headers),
            "payload": payload,
        },
    }


class _Nepse:
    def __init__(self, token_manager, dummy_id_manager):
        self.token_manager = token_manager(self)

        self.dummy_id_manager = dummy_id_manager(
            market_status_function=lambda: self.getMarketStatus()["data"],
            date_function=datetime.now,
        )
        self._tls_verify = True
        self.company_symbol_id_keymap = None
        self.security_symbol_id_keymap = None
        self.company_list = None
        self.security_list = None
        self.sector_scrips = None
        self.floor_sheet_size = 500
        self.base_url = "https://www.nepalstock.com"

        self.load_json_api_end_points()
        self.load_json_dummy_data()
        self.load_json_header()

    ############################################### PRIVATE METHODS###############################################
    def getDummyID(self):
        return self.dummy_id_manager.getDummyID()

    def load_json_header(self):
        json_file_path = f"{pathlib.Path(__file__).parent}/data/HEADERS.json"
        with open(json_file_path, "r") as json_file:
            self.headers = json.load(json_file)
            self.headers["Host"] = self.base_url.replace("https://", "")
            self.headers["Referer"] = self.base_url.replace("https://", "")

    def load_json_api_end_points(self):
        json_file_path = f"{pathlib.Path(__file__).parent}/data/API_ENDPOINTS.json"
        with open(json_file_path, "r") as json_file:
            self.api_end_points = json.load(json_file)

    def get_full_url(self, api_url):
        return f"{self.base_url}{api_url}"

    def load_json_dummy_data(self):
        json_file_path = f"{pathlib.Path(__file__).parent}/data/DUMMY_DATA.json"
        with open(json_file_path, "r") as json_file:
            self.dummy_data = json.load(json_file)

    def getDummyData(self):
        return self.dummy_data

    def init_client(self, tls_verify):
        pass

    def requestGETAPI(self, url, include_authorization_headers=True):
        pass

    def requestPOSTAPI(self, url, payload_generator):
        pass

    def getPOSTPayloadIDForScrips(self):
        pass

    def getPOSTPayloadID(self):
        pass

    def getPOSTPayloadIDForFloorSheet(self):
        pass

    ############################################### PUBLIC METHODS###############################################
    def setTLSVerification(self, flag):
        self._tls_verify = flag
        self.init_client(tls_verify=flag)

    # --- Simple GET endpoints ---
    def getMarketStatus(self):
        return self.requestGETAPI(
            url=self.api_end_points["nepse_open_url"],
        )

    def getPriceVolume(self):
        return self.requestGETAPI(
            url=self.api_end_points["price_volume_url"],
        )

    def getSummary(self):
        return self.requestGETAPI(
            url=self.api_end_points["summary_url"],
        )

    def getTopTenTradeScrips(self):
        return self.requestGETAPI(
            url=self.api_end_points["top_ten_trade_url"],
        )

    def getTopTenTransactionScrips(self):
        return self.requestGETAPI(
            url=self.api_end_points["top_ten_transaction_url"],
        )

    def getTopTenTurnoverScrips(self):
        return self.requestGETAPI(
            url=self.api_end_points["top_ten_turnover_url"],
        )

    def getSupplyDemand(self):
        return self.requestGETAPI(
            url=self.api_end_points["supply_demand_url"],
        )

    def getTopGainers(self):
        return self.requestGETAPI(
            url=self.api_end_points["top_gainers_url"],
        )

    def getTopLosers(self):
        return self.requestGETAPI(
            url=self.api_end_points["top_losers_url"],
        )

    def isNepseOpen(self):
        return self.requestGETAPI(
            url=self.api_end_points["nepse_open_url"],
        )

    def getNepseIndex(self):
        return self.requestGETAPI(
            url=self.api_end_points["nepse_index_url"],
        )

    def getNepseSubIndices(self):
        return self.requestGETAPI(
            url=self.api_end_points["nepse_subindices_url"],
        )

    def getLiveMarket(self):
        return self.requestGETAPI(
            url=self.api_end_points["live-market"],
        )

    # --- POST endpoints ---
    def getPriceVolumeHistory(self, business_date=None):
        if business_date:
            url = f"{self.api_end_points['todays_price']}?size=500&businessDate={business_date}"
        else:
            url = f"{self.api_end_points['todays_price']}?size=500"
        return self.requestPOSTAPI(
            url=url,
            payload_generator=self.getPOSTPayloadIDForFloorSheet,
        )

    def getDailyNepseIndexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["nepse_index_daily_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailySensitiveIndexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["sensitive_index_daily_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyFloatIndexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["float_index_daily_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailySensitiveFloatIndexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["sensitive_float_index_daily_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyBankSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["banking_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyDevelopmentBankSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["development_bank_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyFinanceSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["finance_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyHotelTourismSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["hotel_tourism_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyHydroSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["hydro_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyInvestmentSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["investment_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyLifeInsuranceSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["life_insurance_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyManufacturingSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["manufacturing_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyMicrofinanceSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["microfinance_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyMutualfundSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["mutual_fund_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyNonLifeInsuranceSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["non_life_insurance_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyOthersSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["others_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )

    def getDailyTradingSubindexGraph(self):
        return self.requestPOSTAPI(
            url=self.api_end_points["trading_sub_index_graph"],
            payload_generator=self.getPOSTPayloadID,
        )


class NepseScraper(_Nepse):
    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(TokenManager, DummyIDManager)
        self.init_client(tls_verify=self._tls_verify)

    ############################################### PRIVATE METHODS###############################################
    def getPOSTPayloadIDForScrips(self):
        dummy_id = self.getDummyID()
        e = self.getDummyData()[dummy_id] + dummy_id + 2 * (date.today().day)
        return e

    def getPOSTPayloadID(self):
        e = self.getPOSTPayloadIDForScrips()
        post_payload_id = (
            e
            + self.token_manager.salts[3 if e % 10 < 5 else 1] * date.today().day
            - self.token_manager.salts[(3 if e % 10 < 5 else 1) - 1]
        )
        return post_payload_id

    def getPOSTPayloadIDForFloorSheet(self):
        e = self.getPOSTPayloadIDForScrips()
        post_payload_id = (
            e
            + self.token_manager.salts[1 if e % 10 < 4 else 3] * date.today().day
            - self.token_manager.salts[(1 if e % 10 < 4 else 3) - 1]
        )
        return post_payload_id

    def getAuthorizationHeaders(self):
        access_token = self.token_manager.getAccessToken()
        headers = {
            "Authorization": f"Salter {access_token}",
            "Content-Type": "application/json",
            **self.headers,
        }
        return headers

    def init_client(self, tls_verify):
        self.client = httpx.Client(verify=tls_verify, http2=True, timeout=100)

    def _execute_request(self, method, url, headers, payload=None):
        """Core execution with metadata capture and retry logic"""
        full_url = self.get_full_url(url) if not url.startswith("http") else url
        meta = _create_meta_skeleton(method, full_url, headers, payload)

        start_time = time.perf_counter()
        retry_count = 0
        last_exception = None

        while retry_count < self.MAX_RETRIES:
            try:
                if method == "GET":
                    response = self.client.get(full_url, headers=headers)
                else:
                    response = self.client.post(
                        full_url, headers=headers, data=json.dumps(payload)
                    )

                meta["response_time_ms"] = round(
                    (time.perf_counter() - start_time) * 1000, 2
                )
                meta["http_status"] = response.status_code
                meta["retry_count"] = retry_count

                if 200 <= response.status_code < 300:
                    meta["status"] = "ok"
                    return {"data": response.json(), "meta": meta}
                elif response.status_code == 400:
                    meta["status"] = "error"
                    raise NepseInvalidClientRequest("Bad Request", meta=meta)
                elif response.status_code == 401:
                    meta["status"] = "error"
                    raise NepseTokenExpired("Token Expired", meta=meta)
                elif response.status_code == 502:
                    meta["status"] = "error"
                    raise NepseInvalidServerResponse("Bad Gateway", meta=meta)
                else:
                    meta["status"] = "error"
                    raise NepseNetworkError(f"HTTP {response.status_code}", meta=meta)

            except (
                httpx.RemoteProtocolError,
                httpx.ReadError,
                httpx.ConnectError,
                NepseTokenExpired,
            ) as e:
                retry_count += 1
                last_exception = e

                if isinstance(e, NepseTokenExpired) and retry_count < self.MAX_RETRIES:
                    self.token_manager.update()
                    continue
                elif retry_count >= self.MAX_RETRIES:
                    meta["retry_count"] = retry_count
                    meta["status"] = "error"
                    raise NepseNetworkError(
                        f"Failed after {retry_count} retries: {str(e)}", meta=meta
                    ) from e
                # Otherwise loop continues for network errors

    def requestGETAPI(self, url, include_authorization_headers=True):
        headers = (
            self.getAuthorizationHeaders()
            if include_authorization_headers
            else self.headers
        )
        return self._execute_request("GET", url, headers)

    def requestPOSTAPI(self, url, payload_generator):
        headers = self.getAuthorizationHeaders()
        payload = {"id": payload_generator()}
        return self._execute_request("POST", url, headers, payload)

    ############################################### PUBLIC METHODS###############################################
    def getCompaniesNews(self):
        return self.requestGETAPI(
            url=self.api_end_points["companies_news_url"],
        )

    def getCompanyFinancialReports(
        self,
        symbol: str,
    ):
        symbol = symbol.upper()
        company_id_result = self.getSecurityIDKeyMap()
        # company_id_result is now {"data": {...}, "meta": {...}}
        company_id_map = company_id_result["data"]

        if symbol not in company_id_map:
            meta = _create_meta_skeleton("GET", "N/A", {})
            meta["status"] = "error"
            raise NepseInvalidClientRequest(f"Symbol {symbol} not found", meta=meta)

        company_id = company_id_map[symbol]
        url = f"{self.api_end_points['company_financial_report_url']}{company_id}"
        return self.requestGETAPI(url=url)

    def getCompanyList(self):
        result = self.requestGETAPI(
            url=self.api_end_points["company_list_url"],
        )
        # Cache the data portion for internal use, keep wrapped for return
        self.company_list = result["data"]
        return result

    def getSecurityList(self):
        result = self.requestGETAPI(
            url=self.api_end_points["security_list_url"],
        )
        self.security_list = result["data"]
        return result

    def getSectorScrips(self):
        if self.sector_scrips is None:
            company_list_result = self.getCompanyList()
            company_info_dict = {
                company_info["symbol"]: company_info
                for company_info in company_list_result["data"]
            }

            security_list_result = self.getSecurityList()
            sector_scrips = defaultdict(list)

            for security_info in security_list_result["data"]:
                symbol = security_info["symbol"]
                if company_info_dict.get(symbol):
                    company_info = company_info_dict[symbol]
                    sector_name = company_info["sectorName"]
                    sector_scrips[sector_name].append(symbol)
                else:
                    sector_scrips["Promoter Share"].append(symbol)

            self.sector_scrips = dict(sector_scrips)

        # Construct aggregate metadata
        fetched_at = datetime.now(timezone.utc).isoformat()
        return {
            "data": dict(self.sector_scrips),
            "meta": {
                "source": "nepalstock",
                "fetched_at": fetched_at,
                "status": "ok",
                "http_status": 200,
                "request_id": str(uuid.uuid4()),
                "notes": "Derived from company_list and security_list",
            },
        }

    def getCompanyIDKeyMap(self, force_update=False):
        if self.company_symbol_id_keymap is None or force_update:
            company_list_result = self.getCompanyList()
            company_list = company_list_result["data"]
            self.company_symbol_id_keymap = {
                company["symbol"]: company["id"] for company in company_list
            }

        return {
            "data": self.company_symbol_id_keymap,
            "meta": {
                "source": "nepalstock",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "status": "ok",
                "http_status": 200,
                "request_id": str(uuid.uuid4()),
            },
        }

    def getSecurityIDKeyMap(self, force_update=False):
        if self.security_symbol_id_keymap is None or force_update:
            security_list_result = self.getSecurityList()
            security_list = security_list_result["data"]
            self.security_symbol_id_keymap = {
                security["symbol"]: security["id"] for security in security_list
            }

        return {
            "data": self.security_symbol_id_keymap,
            "meta": {
                "source": "nepalstock",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "status": "ok",
                "http_status": 200,
                "request_id": str(uuid.uuid4()),
            },
        }

    def getCompanyPriceVolumeHistory(self, symbol, start_date=None, end_date=None):
        end_date = end_date if end_date else date.today()
        start_date = start_date if start_date else (end_date - timedelta(days=365))
        symbol = symbol.upper()

        id_map_result = self.getSecurityIDKeyMap()
        if symbol not in id_map_result["data"]:
            raise NepseInvalidClientRequest(f"Symbol {symbol} not found")

        company_id = id_map_result["data"][symbol]
        url = f"{self.api_end_points['company_price_volume_history']}{company_id}?size=500&startDate={start_date}&endDate={end_date}"
        return self.requestGETAPI(url=url)

    def getDailyScripPriceGraph(self, symbol):
        symbol = symbol.upper()
        id_map_result = self.getSecurityIDKeyMap()
        if symbol not in id_map_result["data"]:
            raise NepseInvalidClientRequest(f"Symbol {symbol} not found")

        company_id = id_map_result["data"][symbol]
        return self.requestPOSTAPI(
            url=f"{self.api_end_points['company_daily_graph']}{company_id}",
            payload_generator=self.getPOSTPayloadIDForScrips,
        )

    def getCompanyDetails(self, symbol):
        symbol = symbol.upper()
        id_map_result = self.getSecurityIDKeyMap()
        if symbol not in id_map_result["data"]:
            raise NepseInvalidClientRequest(f"Symbol {symbol} not found")

        company_id = id_map_result["data"][symbol]
        return self.requestPOSTAPI(
            url=f"{self.api_end_points['company_details']}{company_id}",
            payload_generator=self.getPOSTPayloadIDForScrips,
        )

    def getFloorSheet(self, show_progress=False):
        """Aggregated scraper with request chain for paginated floorsheet"""
        url = f"{self.api_end_points['floor_sheet']}?size={self.floor_sheet_size}&sort=contractId,desc"

        all_records = []
        request_chain = []
        total_start = time.perf_counter()

        # Initial request
        first_result = self.requestPOSTAPI(
            url=url,
            payload_generator=self.getPOSTPayloadIDForFloorSheet,
        )

        first_data = first_result["data"]
        request_chain.append(first_result["meta"])

        if "floorsheets" not in first_data:
            # Empty or invalid response
            return {
                "data": [],
                "meta": {
                    **first_result["meta"],
                    "pagination": {
                        "total_records": 0,
                        "pages_fetched": 1,
                        "is_final": True,
                    },
                    "request_chain": request_chain,
                },
            }

        all_records.extend(first_data["floorsheets"]["content"])
        total_pages = first_data["floorsheets"]["totalPages"]

        iterator = (
            tqdm.tqdm(range(1, total_pages)) if show_progress else range(1, total_pages)
        )

        for page_num in iterator:
            page_result = self.requestPOSTAPI(
                url=f"{url}&page={page_num}",
                payload_generator=self.getPOSTPayloadIDForFloorSheet,
            )

            page_data = page_result["data"]
            request_chain.append(page_result["meta"])

            if "floorsheets" in page_data and "content" in page_data["floorsheets"]:
                all_records.extend(page_data["floorsheets"]["content"])

        total_time = round((time.perf_counter() - total_start) * 1000, 2)
        total_retries = sum(m.get("retry_count", 0) for m in request_chain)

        return {
            "data": all_records,
            "meta": {
                "source": "nepalstock",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "status": "ok",
                "http_status": 200,
                "request_id": str(uuid.uuid4()),
                "response_time_ms": total_time,
                "retry_count": total_retries,
                "request": request_chain[0]["request"] if request_chain else {},
                "pagination": {
                    "total_records": len(all_records),
                    "pages_fetched": len(request_chain),
                    "is_final": True,
                },
                "request_chain": request_chain,
            },
        }

    def getFloorSheetOf(self, symbol, business_date=None):
        symbol = symbol.upper()
        business_date = (
            date.fromisoformat(f"{business_date}") if business_date else date.today()
        )

        id_map_result = self.getSecurityIDKeyMap()
        if symbol not in id_map_result["data"]:
            raise NepseInvalidClientRequest(f"Symbol {symbol} not found")

        company_id = id_map_result["data"][symbol]

        all_records = []
        request_chain = []
        total_start = time.perf_counter()

        url_base = f"{self.api_end_points['company_floorsheet']}{company_id}?businessDate={business_date}&size={self.floor_sheet_size}&sort=contractid,desc"

        # First request
        first_result = self.requestPOSTAPI(
            url=url_base,
            payload_generator=self.getPOSTPayloadIDForFloorSheet,
        )

        request_chain.append(first_result["meta"])

        if not first_result["data"]:
            # Empty response
            return {
                "data": [],
                "meta": {
                    **first_result["meta"],
                    "symbol": symbol,
                    "business_date": str(business_date),
                    "pagination": {
                        "total_records": 0,
                        "pages_fetched": 1,
                        "is_final": True,
                    },
                    "request_chain": request_chain,
                },
            }

        first_content = first_result["data"]
        all_records.extend(first_content["floorsheets"]["content"])
        total_pages = first_content["floorsheets"]["totalPages"]

        for page in range(1, total_pages):
            page_result = self.requestPOSTAPI(
                url=f"{url_base}&page={page}",
                payload_generator=self.getPOSTPayloadIDForFloorSheet,
            )
            request_chain.append(page_result["meta"])

            page_content = page_result["data"]
            if "floorsheets" in page_content:
                all_records.extend(page_content["floorsheets"]["content"])

        total_time = round((time.perf_counter() - total_start) * 1000, 2)
        total_retries = sum(m.get("retry_count", 0) for m in request_chain)

        return {
            "data": all_records,
            "meta": {
                "source": "nepalstock",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "status": "ok",
                "http_status": 200,
                "request_id": str(uuid.uuid4()),
                "response_time_ms": total_time,
                "retry_count": total_retries,
                "request": request_chain[0]["request"] if request_chain else {},
                "pagination": {
                    "total_records": len(all_records),
                    "pages_fetched": len(request_chain),
                    "is_final": True,
                },
                "request_chain": request_chain,
                "symbol": symbol,
                "business_date": str(business_date),
            },
        }

    def getSymbolMarketDepth(self, symbol):
        symbol = symbol.upper()
        id_map_result = self.getSecurityIDKeyMap()
        if symbol not in id_map_result["data"]:
            raise NepseInvalidClientRequest(f"Symbol {symbol} not found")

        company_id = id_map_result["data"][symbol]
        url = f"{self.api_end_points['market-depth']}{company_id}/"
        return self.requestGETAPI(url=url)
