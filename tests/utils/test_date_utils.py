from datetime import datetime, timedelta

import pytest

from src.threads.update_stats_per_rank import get_last_time_range
from src.utils import DateUtils


def test_max_date_must_be_greater_than_min_date():
    min_date = datetime.now() - timedelta(days=1)
    max_date = datetime.now()
    with pytest.raises(Exception):
        DateUtils.divide_generic(max_date, min_date, min_date, lambda x: x + timedelta(hours=1))

    DateUtils.divide_generic(min_date, max_date, min_date, lambda x: x + timedelta(hours=1))


@pytest.mark.parametrize(
    "dates,expectation",
    [
        ((datetime(2024, 2, 1), datetime(2024, 3, 1)), ([datetime(2024, 2, 1), datetime(2024, 2, 29, 23, 59, 59)], [])),
        (
            (datetime(2024, 2, 1, 0, 1), datetime(2024, 3, 1)),
            (None, [[datetime(2024, 2, 1, 0, 1), datetime(2024, 3, 1)]]),
        ),
        (
            (datetime(2024, 1, 25), datetime(2024, 8, 3)),
            (
                [datetime(2024, 2, 1), datetime(2024, 7, 31, 23, 59, 59)],
                [
                    [datetime(2024, 1, 25), datetime(2024, 1, 31, 23, 59, 59)],
                    [datetime(2024, 8, 1), datetime(2024, 8, 3)],
                ],
            ),
        ),
    ],
)
def test_divide_range_by_full_months(dates, expectation):
    assert DateUtils.divide_range_by_full_months(dates[0], dates[1]) == expectation


@pytest.mark.parametrize(
    "dates,expectation",
    [
        ((datetime(2024, 2, 1), datetime(2024, 2, 8)), (None, [[datetime(2024, 2, 1), datetime(2024, 2, 8)]])),
        (
            (datetime(2025, 7, 6), datetime(2025, 7, 14)),
            (
                [datetime(2025, 7, 7), datetime(2025, 7, 13, 23, 59, 59)],
                [[datetime(2025, 7, 6), datetime(2025, 7, 6, 23, 59, 59)]],
            ),
        ),
        (
            (datetime(2025, 7, 7), datetime(2025, 7, 14)),
            ([datetime(2025, 7, 7), datetime(2025, 7, 13, 23, 59, 59)], []),
        ),
        (
            (datetime(2025, 7, 1), datetime(2025, 7, 30)),
            (
                [datetime(2025, 7, 7), datetime(2025, 7, 27, 23, 59, 59)],
                [
                    [datetime(2025, 7, 1), datetime(2025, 7, 6, 23, 59, 59)],
                    [datetime(2025, 7, 28), datetime(2025, 7, 30)],
                ],
            ),
        ),
    ],
)
def test_divide_range_by_full_weeks(dates, expectation):
    assert DateUtils.divide_range_by_full_weeks(dates[0], dates[1]) == expectation


@pytest.mark.parametrize(
    "dates,expectation",
    [
        (
            (datetime(2024, 2, 1), datetime(2024, 2, 1, 23, 58)),
            (None, [[datetime(2024, 2, 1), datetime(2024, 2, 1, 23, 58)]]),
        ),
        ((datetime(2025, 7, 6), datetime(2025, 7, 7)), ([datetime(2025, 7, 6), datetime(2025, 7, 6, 23, 59, 59)], [])),
        (
            (datetime(2025, 7, 1, 0, 2), datetime(2025, 7, 30, 22, 58)),
            (
                [datetime(2025, 7, 2), datetime(2025, 7, 29, 23, 59, 59)],
                [
                    [datetime(2025, 7, 1, 0, 2), datetime(2025, 7, 1, 23, 59, 59)],
                    [datetime(2025, 7, 30), datetime(2025, 7, 30, 22, 58)],
                ],
            ),
        ),
    ],
)
def test_divide_range_by_full_days(dates, expectation):
    assert DateUtils.divide_range_by_full_days(dates[0], dates[1]) == expectation


@pytest.mark.parametrize(
    "dates,expectation",
    [
        (
            (datetime(2024, 2, 1, 23), datetime(2024, 2, 1, 23, 58)),
            (None, [[datetime(2024, 2, 1, 23), datetime(2024, 2, 1, 23, 58)]]),
        ),
        (
            (datetime(2025, 7, 6, 23), datetime(2025, 7, 7)),
            ([datetime(2025, 7, 6, 23), datetime(2025, 7, 6, 23, 59, 59)], []),
        ),
        (
            (datetime(2025, 7, 1, 0, 2), datetime(2025, 7, 30, 22, 58)),
            (
                [datetime(2025, 7, 1, 1), datetime(2025, 7, 30, 21, 59, 59)],
                [
                    [datetime(2025, 7, 1, 0, 2), datetime(2025, 7, 1, 0, 59, 59)],
                    [datetime(2025, 7, 30, 22), datetime(2025, 7, 30, 22, 58)],
                ],
            ),
        ),
    ],
)
def test_divide_range_by_full_hours(dates, expectation):
    assert DateUtils.divide_range_by_full_hours(dates[0], dates[1]) == expectation


@pytest.mark.parametrize(
    "input,expectation",
    [
        (("HOURLY", datetime(2025, 7, 9, 12, 22)), (datetime(2025, 7, 9, 11, 0), datetime(2025, 7, 9, 11, 59, 59))),
        (("DAILY", datetime(2025, 7, 9, 12, 22)), (datetime(2025, 7, 8, 0, 0), datetime(2025, 7, 8, 23, 59, 59))),
        (("WEEKLY", datetime(2025, 7, 9, 12, 22)), (datetime(2025, 6, 30), datetime(2025, 7, 6, 23, 59, 59))),
        (("WEEKLY", datetime(2025, 7, 13, 12, 22)), (datetime(2025, 6, 30), datetime(2025, 7, 6, 23, 59, 59))),
        (("MONTHLY", datetime(2025, 7, 9, 12, 22)), (datetime(2025, 6, 1), datetime(2025, 6, 30, 23, 59, 59))),
    ],
)
def test_get_last_time_range(input, expectation):
    assert get_last_time_range(input[0], input[1]) == expectation
