import asyncio
import json
from nepse import Nepse

nepse_manager = Nepse()
nepse_manager.setTLSVerification(False)


def _getCorporateDisclosure():
    result = nepse_manager.getCompanyFinancialReports("hathy")
    print(result)


def _getCompanyDetail():
    result = nepse_manager.getCompanyDetails("hathy")
    print(result)


_getCompanyDetail()
