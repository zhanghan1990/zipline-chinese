
#author zhanghan
'''
This is the trading calendar of Stock in China, in this version
We only consider the day level data
'''
import pandas as pd
import pytz

from datetime import datetime
from dateutil import rrule
from functools import partial

start = pd.Timestamp('1990-01-01', tz='UTC')
end_base = pd.Timestamp('today', tz='UTC')
# Give an aggressive buffer for logic that needs to use the next trading
# day or minute.
end = end_base + pd.Timedelta(days=365)

def canonicalize_datetime(dt):
    # Strip out any HHMMSS or timezone info in the user's datetime, so that
    # all the datetimes we return will be 00:00:00 UTC.
    return datetime(dt.year, dt.month, dt.day, tzinfo=pytz.utc)

def get_non_trading_days(start, end):
    non_trading_rules = []
    start = canonicalize_datetime(start)
    end = canonicalize_datetime(end)

    #this is the rule of saturday and sunday
    weekends = rrule.rrule(
        rrule.YEARLY,
        byweekday=(rrule.SA, rrule.SU),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(weekends)

    #first day of the year
    new_years = rrule.rrule(
        rrule.MONTHLY,
        byyearday=1,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(new_years)

    # 5.1
    may_1st = rrule.rrule(
        rrule.MONTHLY,
        bymonth=5,
        bymonthday=1,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(may_1st)

    #10.1,2,3
    oct_1st=rrule.rrule(
        rrule.MONTHLY,
        bymonth=10,
        bymonthday=1,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(oct_1st)

    oct_2nd=rrule.rrule(
        rrule.MONTHLY,
        bymonth=10,
        bymonthday=2,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(oct_2nd)

    oct_3rd=rrule.rrule(
        rrule.MONTHLY,
        bymonth=10,
        bymonthday=3,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(oct_3rd)

    non_trading_ruleset = rrule.rruleset()

    for rule in non_trading_rules:
        non_trading_ruleset.rrule(rule)

    non_trading_days = non_trading_ruleset.between(start, end, inc=True)

    non_trading_days.append(datetime(1991, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1991, 02, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1991, 02, 18, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1991, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1991, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1991, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1992, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1992, 02, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1992, 02, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1992, 02, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1992, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1992, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1992, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1993, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1993, 01, 25, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1993, 01, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1993, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 02, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 02,  8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 02, 9, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 02, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1994, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 01, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 01, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 02, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 02, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1995, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 20, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 21, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 27, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 28, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 02, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 03, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 9, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1996, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 13, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 02, 14, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 06, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 07, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1997, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 01, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 01, 27, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 01, 28, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 01, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 01, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 02, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 02, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 02, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 02, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1998, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 16, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 17, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 18, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 24, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 25, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 02, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 12, 20, tzinfo=pytz.utc))
    non_trading_days.append(datetime(1999, 12, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 01, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 9, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 05, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 05, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2000, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 24, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 25, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 01, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 02, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 02, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 05, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 05, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2001, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 13, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 14, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 18, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 20, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 21, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 02, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 05, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 05, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 9, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2002, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 01, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 01, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 02, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 02, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 02, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 02, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 05, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 05, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 05, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 05, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 05, 9, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2003, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 20, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 21, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 27, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 01, 28, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 05, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 05, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 05, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 05, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2004, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 02, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 02, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 02, 9, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 02, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 02, 14, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 02, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 05, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 05, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 05, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2005, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 01, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 01, 27, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 01, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 01, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 02, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 02, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 05, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 05, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2006, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 02, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 02, 20, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 02, 21, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 02, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 02, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 05, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 05, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2007, 12, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 02, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 02, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 02, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 02, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 04, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 06, 9, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 9, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 9, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 9, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2008, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 01, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 01, 27, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 01, 28, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 01, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 01, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 04, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 05, 28, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 05, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2009, 10, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 02, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 02, 16, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 02, 17, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 02, 18, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 02, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 04, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 05, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 06, 14, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 06, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 06, 16, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 9, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 9, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 9, 24, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2010, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 02, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 02, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 02, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 02, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 04, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 04, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 06, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 9, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2011, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 01, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 01, 24, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 01, 25, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 01, 26, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 01, 27, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 04, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 04, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 04, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 04, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 06, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2012, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 01, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 02, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 02, 13, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 02, 14, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 02, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 04, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 04, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 04, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 04, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 06, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 06, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 06, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 9, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 9, 20, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2013, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 01, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 02, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 02, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 02, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 02, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 04, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 06, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 9, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2014, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 02, 18, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 02, 19, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 02, 20, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 02, 23, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 02, 24, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 04, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 06, 22, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 9, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 9, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 10, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2015, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 01, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 02, 8, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 02, 9, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 02, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 02, 11, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 02, 12, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 04, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 05, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 06, 9, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 06, 10, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 9, 15, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 9, 16, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 10, 06, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2016, 10, 07, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 01, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 01, 27, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 01, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 01, 31, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 02, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 02, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 04, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 04, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 05, 01, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 05, 29, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 05, 30, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 10, 02, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 10, 03, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 10, 04, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 10, 05, tzinfo=pytz.utc))
    non_trading_days.append(datetime(2017, 10, 06, tzinfo=pytz.utc))
    
    non_trading_days.sort()

    return pd.DatetimeIndex(non_trading_days)

non_trading_days = get_non_trading_days(start, end)
trading_day = pd.tseries.offsets.CDay(holidays=non_trading_days)


def get_trading_days(start, end, trading_day=trading_day):
    return pd.date_range(start=start.date(),
                         end=end.date(),
                         freq=trading_day).tz_localize('UTC')

trading_days = get_trading_days(start, end)


def get_early_closes(start, end):
    # 1:00 PM close rules based on
    # http://quant.stackexchange.com/questions/4083/nyse-early-close-rules-july-4th-and-dec-25th # noqa
    # and verified against http://www.nyse.com/pdfs/closings.pdf

    # These rules are valid starting in 1993

    start = canonicalize_datetime(start)
    end = canonicalize_datetime(end)

    start = max(start, datetime(1993, 1, 1, tzinfo=pytz.utc))
    end = max(end, datetime(1993, 1, 1, tzinfo=pytz.utc))

    # Not included here are early closes prior to 1993
    # or unplanned early closes

    early_close_rules = []

    day_after_thanksgiving = rrule.rrule(
        rrule.MONTHLY,
        bymonth=11,
        # 4th Friday isn't correct if month starts on Friday, so restrict to
        # day range:
        byweekday=(rrule.FR),
        bymonthday=range(23, 30),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(day_after_thanksgiving)

    christmas_eve = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=24,
        byweekday=(rrule.MO, rrule.TU, rrule.WE, rrule.TH),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(christmas_eve)

    friday_after_christmas = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=26,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        # valid 1993-2007
        until=min(end, datetime(2007, 12, 31, tzinfo=pytz.utc))
    )
    early_close_rules.append(friday_after_christmas)

    day_before_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=(rrule.MO, rrule.TU, rrule.TH),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(day_before_independence_day)

    day_after_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=5,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        # starting in 2013: wednesday before independence day
        until=min(end, datetime(2012, 12, 31, tzinfo=pytz.utc))
    )
    early_close_rules.append(day_after_independence_day)

    wednesday_before_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=rrule.WE,
        cache=True,
        # starting in 2013
        dtstart=max(start, datetime(2013, 1, 1, tzinfo=pytz.utc)),
        until=max(end, datetime(2013, 1, 1, tzinfo=pytz.utc))
    )
    early_close_rules.append(wednesday_before_independence_day)

    early_close_ruleset = rrule.rruleset()

    for rule in early_close_rules:
        early_close_ruleset.rrule(rule)
    early_closes = early_close_ruleset.between(start, end, inc=True)

    # Misc early closings from NYSE listing.
    # http://www.nyse.com/pdfs/closings.pdf
    #
    # New Year's Eve
    nye_1999 = datetime(1999, 12, 31, tzinfo=pytz.utc)
    if start <= nye_1999 and nye_1999 <= end:
        early_closes.append(nye_1999)

    early_closes.sort()
    return pd.DatetimeIndex(early_closes)

early_closes = get_early_closes(start, end)


def get_open_and_close(day, early_closes):
    market_open = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=9,
            minute=31),
        tz='US/Eastern').tz_convert('UTC')
    # 1 PM if early close, 4 PM otherwise
    close_hour = 13 if day in early_closes else 16
    market_close = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=close_hour),
        tz='Asia/Shanghai').tz_convert('UTC')

    return market_open, market_close


def get_open_and_closes(trading_days, early_closes, get_open_and_close):
    open_and_closes = pd.DataFrame(index=trading_days,
                                   columns=('market_open', 'market_close'))

    get_o_and_c = partial(get_open_and_close, early_closes=early_closes)

    open_and_closes['market_open'], open_and_closes['market_close'] = \
        zip(*open_and_closes.index.map(get_o_and_c))

    return open_and_closes

open_and_closes = get_open_and_closes(trading_days, early_closes,
                                      get_open_and_close)
