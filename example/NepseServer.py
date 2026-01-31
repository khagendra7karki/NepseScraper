from nepse_scraper import NepseScraper

nepse_manager = NepseScraper()
nepse_manager.setTLSVerification(False)


def _getTodayPrice():
    today = "2026-01-31"
    result = nepse_manager.getPriceVolumeHistory()
    print(result)


try:
    _getTodayPrice()
except Exception as exc:
    print(exc.__getattribute__("meta"))
