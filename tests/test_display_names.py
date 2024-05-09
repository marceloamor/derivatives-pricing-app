from datetime import datetime

import pytest
import upedata.enums as upeenums
import upedata.static_data as upestatic

from src import display_names


@pytest.mark.parametrize(
    ["option_sd", "expected_output"],
    [
        [
            upestatic.Option(
                symbol="test base a",
                display_name=r"tba1 @{strike}£ @{call_or_put}£",
                product_symbol="",
                vol_surface_id=1,
                underlying_future_symbol="",
                strike_intervals=[[1, 1], [10, -10]],
                time_type=upeenums.TimeType.IGNORE_WEEKENDS,
                multiplier=1,
                vol_type=upeenums.VolType.STANDARD,
                expiry=datetime(1970, 1, 1),
            ),
            {
                "test base a-0.1-c": "tba1 0.1 c",
                "test base a-0.2-c": "tba1 0.2 c",
                "test base a-0.3-c": "tba1 0.3 c",
                "test base a-0.4-c": "tba1 0.4 c",
                "test base a-0.5-c": "tba1 0.5 c",
                "test base a-0.6-c": "tba1 0.6 c",
                "test base a-0.7-c": "tba1 0.7 c",
                "test base a-0.8-c": "tba1 0.8 c",
                "test base a-0.9-c": "tba1 0.9 c",
                "test base a-1-c": "tba1 1 c",
                "test base a-0.1-p": "tba1 0.1 p",
                "test base a-0.2-p": "tba1 0.2 p",
                "test base a-0.3-p": "tba1 0.3 p",
                "test base a-0.4-p": "tba1 0.4 p",
                "test base a-0.5-p": "tba1 0.5 p",
                "test base a-0.6-p": "tba1 0.6 p",
                "test base a-0.7-p": "tba1 0.7 p",
                "test base a-0.8-p": "tba1 0.8 p",
                "test base a-0.9-p": "tba1 0.9 p",
                "test base a-1-p": "tba1 1 p",
            },
        ],
        [
            upestatic.Option(
                symbol="test base a",
                display_name=r"tba1 @{strike}£ @{call_or_put}£",
                product_symbol="",
                vol_surface_id=1,
                underlying_future_symbol="",
                strike_intervals=[[5, 5], [50, -10]],
                time_type=upeenums.TimeType.IGNORE_WEEKENDS,
                multiplier=1,
                vol_type=upeenums.VolType.STANDARD,
                expiry=datetime(1970, 1, 1),
            ),
            {
                "test base a-0.5-c": "tba1 0.5 c",
                "test base a-1-c": "tba1 1 c",
                "test base a-1.5-c": "tba1 1.5 c",
                "test base a-2-c": "tba1 2 c",
                "test base a-2.5-c": "tba1 2.5 c",
                "test base a-3-c": "tba1 3 c",
                "test base a-3.5-c": "tba1 3.5 c",
                "test base a-4-c": "tba1 4 c",
                "test base a-4.5-c": "tba1 4.5 c",
                "test base a-5-c": "tba1 5 c",
                "test base a-0.5-p": "tba1 0.5 p",
                "test base a-1-p": "tba1 1 p",
                "test base a-1.5-p": "tba1 1.5 p",
                "test base a-2-p": "tba1 2 p",
                "test base a-2.5-p": "tba1 2.5 p",
                "test base a-3-p": "tba1 3 p",
                "test base a-3.5-p": "tba1 3.5 p",
                "test base a-4-p": "tba1 4 p",
                "test base a-4.5-p": "tba1 4.5 p",
                "test base a-5-p": "tba1 5 p",
            },
        ],
        [
            upestatic.Option(
                symbol="test base a",
                display_name=None,
                product_symbol="",
                vol_surface_id=1,
                underlying_future_symbol="",
                strike_intervals=[[1, 1], [10, -10]],
                time_type=upeenums.TimeType.IGNORE_WEEKENDS,
                multiplier=1,
                vol_type=upeenums.VolType.STANDARD,
                expiry=datetime(1970, 1, 1),
            ),
            {
                "test base a-0.1-c": "test base a-0.1-c",
                "test base a-0.2-c": "test base a-0.2-c",
                "test base a-0.3-c": "test base a-0.3-c",
                "test base a-0.4-c": "test base a-0.4-c",
                "test base a-0.5-c": "test base a-0.5-c",
                "test base a-0.6-c": "test base a-0.6-c",
                "test base a-0.7-c": "test base a-0.7-c",
                "test base a-0.8-c": "test base a-0.8-c",
                "test base a-0.9-c": "test base a-0.9-c",
                "test base a-1-c": "test base a-1-c",
                "test base a-0.1-p": "test base a-0.1-p",
                "test base a-0.2-p": "test base a-0.2-p",
                "test base a-0.3-p": "test base a-0.3-p",
                "test base a-0.4-p": "test base a-0.4-p",
                "test base a-0.5-p": "test base a-0.5-p",
                "test base a-0.6-p": "test base a-0.6-p",
                "test base a-0.7-p": "test base a-0.7-p",
                "test base a-0.8-p": "test base a-0.8-p",
                "test base a-0.9-p": "test base a-0.9-p",
                "test base a-1-p": "test base a-1-p",
            },
        ],
    ],
)
def test_process_option_data_to_display_name_df(option_sd, expected_output):
    display_names_map = display_names._process_option_data_to_display_name_map(
        option_sd
    )

    assert display_names_map == expected_output
