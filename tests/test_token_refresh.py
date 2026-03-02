from datetime import datetime, timedelta
import time
from nepse_scraper import NepseScraper
from nepse_scraper.models.todays_price import TodaysPriceResponse


def test_token_refresh():
    nepse_manager = NepseScraper()
    nepse_manager.setTLSVerification(False)
    start = datetime.now()
    while (datetime.now() - start) < timedelta(seconds=60):
        result = nepse_manager.getPriceVolumeHistory()
        assert "data" in result
        assert "meta" in result

        assert TodaysPriceResponse.model_validate(result["data"])

        # Sleep
        time.sleep(1.0)
