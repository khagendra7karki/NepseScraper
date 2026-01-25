import asyncio
import json
from nepse import AsyncNepse

nepse_manager = AsyncNepse()
nepse_manager.setTLSVerification(False)


async def _getCorporateDisclosure():
    result = await nepse_manager.getCompanyFinancialReports("hathy")
    print(result)


async def _getCompanyDetail():
    result = await nepse_manager.getCompanyDetails("hathy")
    print(result)


asyncio.run(_getCompanyDetail())
