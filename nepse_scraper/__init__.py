from nepse_scraper.NepseLib import NepseScraper


# function added to reduce namespace pollution (importing datetime)
def timestamp(year, month, date):
    import datetime

    return datetime.date(year, month, date)


__all__ = [
    "NepseScraper",
]

__version__ = "0.0.1"
__release_date__ = timestamp(2026, 1, 31)
