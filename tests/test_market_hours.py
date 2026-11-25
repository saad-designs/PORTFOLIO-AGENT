import datetime

from portfolio_manager import is_psx_market_open


PKT = datetime.timezone(datetime.timedelta(hours=5))


def _pkt_datetime(year, month, day, hour, minute):
    return datetime.datetime(year, month, day, hour, minute, tzinfo=PKT)


def test_official_weekday_session_boundaries():
    assert is_psx_market_open(_pkt_datetime(2026, 11, 23, 9, 31)) is False
    assert is_psx_market_open(_pkt_datetime(2026, 11, 23, 9, 32)) is True
    assert is_psx_market_open(_pkt_datetime(2026, 11, 23, 15, 29)) is True
    assert is_psx_market_open(_pkt_datetime(2026, 11, 23, 15, 30)) is False


def test_official_friday_two_session_hours():
    assert is_psx_market_open(_pkt_datetime(2026, 11, 27, 9, 16)) is False
    assert is_psx_market_open(_pkt_datetime(2026, 11, 27, 9, 17)) is True
    assert is_psx_market_open(_pkt_datetime(2026, 11, 27, 12, 0)) is False
    assert is_psx_market_open(_pkt_datetime(2026, 11, 27, 14, 32)) is True
    assert is_psx_market_open(_pkt_datetime(2026, 11, 27, 16, 30)) is False


def test_psx_holiday_and_weekend_are_closed():
    assert is_psx_market_open(_pkt_datetime(2026, 2, 5, 10, 0)) is False
    assert is_psx_market_open(_pkt_datetime(2026, 11, 21, 10, 0)) is False


def test_unlisted_calendar_year_is_unverified():
    assert is_psx_market_open(_pkt_datetime(2028, 1, 4, 10, 0)) is None


def test_2027_trading_day_and_fixed_holiday():
    assert is_psx_market_open(_pkt_datetime(2027, 1, 4, 10, 0)) is True
    assert is_psx_market_open(_pkt_datetime(2027, 3, 23, 10, 0)) is False


def test_aware_utc_time_is_converted_to_karachi():
    utc = datetime.datetime(2026, 11, 23, 4, 32, tzinfo=datetime.timezone.utc)
    assert is_psx_market_open(utc) is True
