from nepse_scraper import NepseScraper

nepse_manager = NepseScraper()
nepse_manager.setTLSVerification(False)


def _getCorporateDisclosure():
    result = nepse_manager.getCompanyFinancialReports("hathy")
    print(result)


def _getCompanyDetail():
    result = nepse_manager.getCompanyDetails("hathy")
    print(result)


_getCompanyDetail()
