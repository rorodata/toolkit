import re
from .imports import lazy_import

pd = lazy_import("pandas")

def truncate_date(date, timescale: str):
    """Truncates a date to nearest week or month.

    Parameters
    ----------
    date: the date to truncate
    timescale: one of day/week/month or daily/weekly/monthly
    """
    date = pd.Timestamp(date)
    if timescale in ['week', 'weekly']:
        return date - pd.Timedelta(days=date.dayofweek)
    elif timescale in ['month', 'monthly']:
        return date.replace(day=1)
    else:
        return date

def relative_date(date_string, reference_date):
    m = re.match(r"^(date|week|month)([+-]\d+)$", date_string)
    if not m:
        raise ValueError("Invalid date: ", date_string)
    timescale, n = m.groups()
    reference_date = truncate_date(reference_date, timescale).date()
    n = int(n)
    if n == 0:
        date = reference_date
    else:
        step = {"week": "W-MON", "month": "MS", "date": "D"}[timescale]
        freq = "{}{}".format(n, step)
        date = pd.date_range(reference_date, periods=2, freq=freq)[-1].date()
    return date
