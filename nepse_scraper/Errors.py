# nepse_scraper/Errors.py


class ScrapingError(Exception):
    """Base exception with Request/Response context attached"""

    def __init__(self, message, meta=None):
        super().__init__(message)
        self.meta = meta or {}


class NepseInvalidServerResponse(ScrapingError):
    pass


class NepseInvalidClientRequest(ScrapingError):
    pass


class NepseNetworkError(ScrapingError):
    pass


class NepseTokenExpired(ScrapingError):
    pass
