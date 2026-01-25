from nepse.NepseLib import AsyncNepse


# function added to reduce namespace pollution (importing datetime)
def timestamp(year, month, date):
    import datetime

    return datetime.date(year, month, date)


__all__ = [
    "AsyncNepse",
]

__version__ = "0.6.2"
__release_date__ = timestamp(2025, 1, 25)
