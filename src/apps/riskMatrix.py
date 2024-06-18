import datetime as dt
import json, os
import logging
from datetime import datetime
import time

import colorlover
import dash_bootstrap_components as dbc
import orjson
import pandas as pd
import requests
import sqlalchemy
import upedata.static_data as upe_static
import upedata.dynamic_data as upe_dynamic
from dash import dash_table as dtable
from dash import dcc, html, no_update
import dash_daq as daq
from dash.dependencies import Input, Output, State
from data_connections import conn, riskAPi, shared_session
from dateutil import relativedelta
from parts import (
    onLoadPortFolio,
    topMenu,
    loadPortfolios,
)
from zoneinfo import ZoneInfo

from icecream import ic

logger = logging.getLogger("frontend")

new_risk_api = os.getenv("RISK_API")

undSteps = {
    "aluminium": "10",
    "copper": "40",
    "nickel": "100",
    "zinc": "10",
    "lead": "10",
}


def buildURL(base, portfolio, und, vol, level, eval, rels):
    und = "und=" + str(und)[1:-1]
    vol = "vol=" + str(vol)[1:-1]
    level = "level=" + level
    portfolio = "portfolio=" + portfolio
    rels = "rel=" + rels
    eval = "eval=" + eval

    url = (
        base
        + "?"
        + portfolio
        + "&"
        + vol
        + "&"
        + und
        + "&"
        + level
        + "&"
        + eval
        + "&"
        + rels
    )
    logger.debug(url)
    return url


def discrete_background_color_bins(df, n_bins=4, columns="all"):
    bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]

    if columns == "all":
        if "id" in df:
            df_numeric_columns = df.select_dtypes("number").drop(["id"], axis=1)
        else:
            df_numeric_columns = df.select_dtypes("number")
    else:
        df_numeric_columns = df[columns]

    df_max = df_numeric_columns.max().max()
    df_min = df_numeric_columns.min().min()

    styles = []

    # build ranges
    ranges = [(df_max * i) for i in bounds]

    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(bounds))
        backgroundColor = colorlover.scales[half_bins]["seq"]["Greens"][i - 1]
        color = "black"
        for column in df_numeric_columns:
            styles.append(
                {
                    "if": {
                        "filter_query": (
                            "{{{column}}} >= {min_bound}"
                            + (
                                " && {{{column}}} < {max_bound}"
                                if (i < len(ranges) - 1)
                                else ""
                            )
                        ).format(
                            column=column, min_bound=min_bound, max_bound=max_bound
                        ),
                        "column_id": str(column),
                    },
                    "backgroundColor": backgroundColor,
                    "color": color,
                }
            )

    # build ranges
    ranges = [(df_min * i) for i in bounds]
    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(ranges))
        backgroundColor = colorlover.scales[half_bins]["seq"]["Reds"][i - 1]
        color = "black"
        for column in df_numeric_columns:
            styles.append(
                {
                    "if": {
                        "filter_query": (
                            "{{{column}}} <= {min_bound}"
                            + (
                                " && {{{column}}} > {max_bound}"
                                if (i < len(ranges) - 1)
                                else ""
                            )
                        ).format(
                            column=column, min_bound=min_bound, max_bound=max_bound
                        ),
                        "column_id": str(column),
                    },
                    "backgroundColor": backgroundColor,
                    "color": color,
                }
            )

    # add zero color
    for column in df_numeric_columns:
        styles.append(
            {
                "if": {
                    "filter_query": ("{{{column}}} = 0").format(column=column),
                    "column_id": str(column),
                },
                "backgroundColor": "rgb(255,255,255)",
                "color": color,
            }
        )
    return styles


# create table per greek
def create_greek_table(parsed_data, metric):
    table_data = {ps["price_shock"]: [] for ps in parsed_data[0]["price_shocks"]}
    index = []

    for entry in parsed_data:
        index.append(entry["eval_time"])
        for ps in entry["price_shocks"]:
            table_data[ps["price_shock"]].append(ps[metric])

    df = pd.DataFrame(table_data, index=index)

    df.reset_index(inplace=True)
    df.rename(columns={"index": "eval_datetime"}, inplace=True)
    if metric == "market_value":
        base_value = df.loc[0, 0]
        # subtract base_value from all values
        numeric_cols = df.select_dtypes("number").columns
        df[numeric_cols] = df[numeric_cols].apply(lambda x: x - base_value)
    df = df.round(rounding_dict[metric])
    return df


def create_all_greeks_table_one_date(
    parsed_data,
    fields,
    date_index=0,
):
    first_date_data = parsed_data[date_index]["price_shocks"]
    table_data = {ps["price_shock"]: [] for ps in first_date_data}
    index = fields

    for field in fields:
        for ps in first_date_data:
            table_data[ps["price_shock"]].append(ps[field])

    df = pd.DataFrame(table_data, index=index)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "greek"}, inplace=True)

    # subtract base value from all values in market_value row
    base_value = df.loc[0, 0]
    mask = df["greek"] == "market_value"
    df.loc[mask, df.columns != "greek"] = (
        df.loc[mask, df.columns != "greek"] - base_value
    )

    # custom rounding fuction per row
    def round_row(row):
        greek = row["greek"]
        decimals = rounding_dict.get(greek, 0)
        return row.apply(
            lambda x: round(x, decimals) if isinstance(x, (int, float)) else x
        )

    df = df.apply(round_row, axis=1)

    return df


def convert_times_to_timezone_aware(open_time_str, close_time_str, locale_str):
    # Define the open and close times
    open_time = datetime.strptime(open_time_str, "%H:%M").time()
    close_time = datetime.strptime(close_time_str, "%H:%M").time()

    # Create datetime objects for today with the given times
    today = datetime.now().date()
    open_datetime = datetime.combine(today, open_time)
    close_datetime = datetime.combine(today, close_time)

    # Make these datetime objects timezone-aware in the given locale
    locale_tz = ZoneInfo(locale_str)
    open_datetime = open_datetime.replace(tzinfo=locale_tz)
    close_datetime = close_datetime.replace(tzinfo=locale_tz)

    # Get the local timezone dynamically
    local_tz_name = time.tzname[time.localtime().tm_isdst]  # Get the timezone name
    local_tz = ZoneInfo(local_tz_name)

    # Convert to the current timezone
    open_datetime_in_current_tz = open_datetime.astimezone(local_tz)
    close_datetime_in_current_tz = close_datetime.astimezone(local_tz)

    return open_datetime_in_current_tz, close_datetime_in_current_tz


def loadRiskPortfolios():
    with shared_session() as session:
        portfolio_options = session.query(upe_static.Portfolio).all()
        portfolio_options = [
            {"label": x.display_name, "value": x.portfolio_id}
            for x in portfolio_options
            if x.display_name != "Error"
            and x.display_name != "Backbook"
            and x.display_name != "CME General"
        ]
    return portfolio_options


greek_options = [
    {"label": "PnL", "value": "market_value"},
    {"label": "Deltas", "value": "deltas"},
    {"label": "Skew Delta", "value": "skew_deltas"},
    {"label": "Vega", "value": "vegas"},
    {"label": "Theta", "value": "thetas"},
    {"label": "Gamma", "value": "gammas"},
    {"label": "Skew Gamma", "value": "skew_gammas"},
    {"label": "Delta Decay", "value": "delta_decays"},
    {"label": "Vega Decay", "value": "vega_decays"},
    {"label": "Gamma Decay", "value": "gamma_decays"},
]

rounding_dict = {
    "market_value": 0,
    "deltas": 1,
    "skew_deltas": 1,
    "vegas": 0,
    "thetas": 0,
    "gammas": 3,
    "skew_gammas": 3,
    "delta_decays": 1,
    "vega_decays": 0,
    "gamma_decays": 3,
}


product_options = dbc.Row(
    [
        dbc.Col(
            [
                html.Label(
                    ["Portfolio(s):"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                dcc.Dropdown(
                    id="risk-portfolio-dropdown",
                    options=loadRiskPortfolios(),
                    multi=True,
                    placeholder="Select Portfolio(s)",
                    # value=,
                ),
            ],
            width=3,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Product:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                dcc.Dropdown(
                    id="risk-product-dropdown",
                    options=[],
                    # value="copper",
                ),
                html.Br(),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Br(),
                html.Label(
                    [""],
                    style={"font-weight": "bold", "text-align": "left"},
                    id="basis-price-label",
                ),
                html.Br(),
            ],
            width=2,
        ),
    ]
)

calendar_options = dbc.Row(
    [
        dbc.Col(
            [
                html.Label(
                    ["SoD / Live / EoD:"],
                    style={"font-weight": "bold", "text-align": "center"},
                ),
                dcc.Slider(
                    -1,
                    1,
                    1,
                    value=0,
                    id="time_of_day_slider",
                    marks={-1: "Start", 0: "Live", 1: "End"},
                ),
                html.Label(
                    ["Evaluation Dates:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                dcc.DatePickerRange(
                    id="risk-matrix-eval-date",
                    month_format="MMMM Y",
                    start_date=dt.datetime.today(),
                    end_date=dt.datetime.today() + dt.timedelta(days=5),
                ),
                html.Br(),
            ],
            width=3,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Shock Size:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div(
                    [
                        dcc.Input(
                            id="shock-size-input",
                            type="number",
                            placeholder=50,
                        )
                    ],
                ),
                html.Label(
                    ["# of Shocks:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div(
                    [
                        dcc.Input(
                            id="shock-number-input",
                            type="number",
                            placeholder=5,
                        )
                    ],
                ),
            ],
            width=3,
        ),
        dbc.Col(
            [
                html.Br(),
                html.Div(
                    dbc.Button("generate!", id="generate-risk-button", n_clicks=0)
                ),
            ],
            width=1,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Greek:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                dcc.Dropdown(
                    id="risk-matrix-greek-dropdown",
                    options=greek_options,
                    value="market_value",
                ),
                html.Br(),
            ],
            width=2,
        ),
    ]
)


options = dbc.Row(
    [
        product_options,
        html.Br(),
        calendar_options,
        html.Br(),
        # shock_options,
        html.Br(),
    ]
)

old_options = dbc.Row(
    [
        dbc.Col(
            [
                html.Label(
                    ["Portfolio:"], style={"font-weight": "bold", "text-align": "left"}
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dcc.Dropdown(
                                    id="riskPortfolio",
                                    options=onLoadPortFolio(),
                                    value="copper",
                                )
                            ]
                        )
                    ]
                ),
                html.Label(
                    ["Basis Price:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Div(
                                    [
                                        dcc.Input(
                                            id="basisPrice",
                                            type="number",
                                        )
                                    ]
                                ),
                            ]
                        )
                    ]
                ),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Price Shock Step Size:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div([dcc.Input(id="shockSize", type="number")]),
                html.Label(
                    ["Price Shock Max:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div([dcc.Input(id="shockMax", type="number")]),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Time Step Size:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div([dcc.Input(id="timeStepSize", placeholder=1, type="number")]),
                html.Label(
                    ["Time Max:"], style={"font-weight": "bold", "text-align": "left"}
                ),
                html.Div([dcc.Input(id="timeMax", placeholder=10, type="number")]),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Label(
                    ["Evaluation Date:"],
                    style={"font-weight": "bold", "text-align": "left"},
                ),
                html.Div(
                    [
                        dcc.DatePickerSingle(
                            id="evalDate",
                            month_format="MMMM Y",
                            placeholder="MMMM Y",
                            date=dt.datetime.today(),
                        )
                    ],
                ),
                html.Br(),
                html.Div(dbc.Button("generate!", id="riskMatrix-button", n_clicks=0)),
            ],
            width=2,
        ),
        dbc.Col(
            [
                html.Br(),
                html.Label(
                    ["Greek:"], style={"font-weight": "bold", "text-align": "left"}
                ),
                dcc.Dropdown(
                    id="greeks",
                    options=[
                        {
                            "label": "Full Delta",
                            "value": "full_delta",
                        },
                        {"label": "Delta", "value": "delta"},
                        {"label": "Vega", "value": "vega"},
                        {"label": "Gamma", "value": "gamma"},
                        {"label": "Theta", "value": "theta"},
                        {"label": "PnL", "value": "position_value"},
                    ],
                    value="full_delta",
                ),
            ],
            width=2,
        ),
    ]
)


old_heatMap = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Loading(
                    id="loading-2",
                    type="circle",
                    children=[
                        dtable.DataTable(
                            id="riskMatrix",
                            data=[{}],
                            # fixed_columns={'headers': True, 'data': 1},
                            style_table={"overflowX": "scroll", "minWidth": "100%"},
                        )
                    ],
                )
            ]
        )
    ]
)
heatMap = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Loading(
                    id="loading-2",
                    type="circle",
                    children=[
                        html.Div(
                            [],
                            id="risk-matrix-single-greek-table",
                            style={"overflowX": "scroll"},
                        )
                    ],
                )
            ]
        )
    ]
)

old_greeksTable = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Loading(
                    id="loading-2",
                    type="circle",
                    children=[
                        dtable.DataTable(
                            id="greeksTable",
                            data=[{}],
                            # fixed_columns={"headers": True, "data": 1},
                            style_table={"overflowX": "scroll", "minWidth": "100%"},
                        )
                    ],
                )
            ]
        )
    ]
)

greeksTable = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Loading(
                    id="loading-2",
                    type="circle",
                    children=[
                        html.Div(
                            [],
                            id="risk-matrix-single-date-table",
                            style={"overflowX": "scroll"},
                        )
                    ],
                )
            ]
        )
    ]
)


old_hidden = dbc.Row([dcc.Store(id="riskData")])
hidden = dbc.Row([dcc.Store(id="risk-data-store")])
hidden_payload = dbc.Row([dcc.Store(id="risk-data-payload-store")])

grey_divider = html.Hr(
    style={
        "width": "100%",
        "borderTop": "2px solid gray",
        "borderBottom": "2px solid gray",
        "opacity": "unset",
    }
)

layout = html.Div(
    [
        topMenu("Risk Matrix"),
        options,
        html.Br(),
        grey_divider,
        heatMap,
        html.Br(),
        greeksTable,
        # html.Div([], id="riskMatrixOutputDiv"),
        html.Br(),
        grey_divider,
        html.Br(),
        # old_options,
        old_heatMap,
        html.Br(),
        old_greeksTable,
        old_hidden,
        hidden,
        hidden_payload,
    ]
)


def placholderCheck(value, placeholder):
    if value and value != None:
        return float(value)

    elif placeholder and placeholder != None:
        return float(placeholder)


def initialise_callbacks(app):
    # risk matrix heat map
    @app.callback(
        Output("risk-product-dropdown", "options"),
        Input("risk-portfolio-dropdown", "value"),
        # prevent_initial_call=True,
    )
    def load_product_dropdown(selected_portfolios):
        # pull distinct product from positions in selected portfolios
        with shared_session() as session:
            if selected_portfolios:
                query = (
                    sqlalchemy.select(upe_dynamic.Position.instrument_symbol)
                    .where(upe_dynamic.Position.portfolio_id.in_(selected_portfolios))
                    .where(upe_dynamic.Position.net_quantity != 0)
                    .distinct(upe_dynamic.Position.instrument_symbol)
                )
            else:
                query = (
                    sqlalchemy.select(upe_dynamic.Position.instrument_symbol)
                    .where(upe_dynamic.Position.net_quantity != 0)
                    .distinct(upe_dynamic.Position.instrument_symbol)
                )
            product_symbols = session.execute(query).all()

            product_symbols = set([x[0].split(" ")[0] for x in product_symbols])

            query = (
                sqlalchemy.select(
                    upe_static.Product.long_name, upe_static.Product.symbol
                )
                .where(upe_static.Product.symbol.in_(product_symbols))
                .distinct(upe_static.Product.symbol)
            )

            product_options = session.execute(query).all()

            product_options = [
                {"label": x.long_name, "value": x.symbol} for x in product_options
            ]

            product_options.sort(key=lambda x: x["value"])

            return product_options

    # date picker boolean switch
    @app.callback(
        Output("time_of_day_slider", "marks"),
        Output("basis-price-label", "children"),
        Input("risk-product-dropdown", "value"),
        prevent_initial_call=True,
    )
    def load_product_info(product):
        if product:
            # pull market hours from db and basis price from db/redis
            with shared_session() as session:
                select_front_month_symbol = (
                    sqlalchemy.select(upe_static.Option.symbol)
                    .where(upe_static.Option.product_symbol == product)
                    .where(
                        upe_static.Option.expiry
                        >= (
                            datetime.now(tz=ZoneInfo("UTC"))
                            + relativedelta.relativedelta(days=1)
                        )
                    )
                    .order_by(
                        upe_static.Option.product_symbol.asc(),
                        upe_static.Option.expiry.asc(),
                    )
                    .distinct(upe_static.Option.product_symbol)
                )
                front_month_symbol = session.execute(
                    select_front_month_symbol
                ).scalar_one_or_none()
                if front_month_symbol is None:
                    logger.error(
                        "Unable to find front month option symbol for %s", product
                    )
                    return 0.0, 0.0, 0.0, "", "", ""
                month = conn.get(front_month_symbol)
                option_data = pd.DataFrame(orjson.loads(month))

                atm = float(option_data.iloc[0]["underlying_prices"])
                ic(atm)
                spread = option_data.iloc[0]["spread"]
                forward = round(atm + spread, 2)
                ic(spread)
                ic(forward)
                basis_string = f"Basis Price: {forward}"

                query = (
                    sqlalchemy.select(
                        upe_static.Product.market_open_naive,
                        upe_static.Product.market_close_naive,
                        upe_static.Product.locale,
                    )
                    .where(upe_static.Product.symbol == product)
                    .distinct(upe_static.Product.symbol)
                )
                market_hours = session.execute(query).all()
                open, close, locale = market_hours[0]

                # turn open and close datetimes into locale specific time zone aware datetimes
                # open = datetime.combine(datetime.today(), open)
                # close = datetime.combine(datetime.today(), close)
                # I have two datetimes, one for open and one for close, and a locale string
                # ic(open, close, locale)

                # format date string
                open = open.strftime("%H:%M")
                close = close.strftime("%H:%M")

                # locale = ZoneInfo(locale)
                # now_datetime = datetime.now(tz=locale)
                # now_date = now_datetime.date()
                # tz_aware_open = datetime.combine(now_date, open, locale)
                # this is where i need to convert the open and close times to the correct time zone using locale information

                # ic(tz_aware_open)
                # ic(close_datetime)

                now = datetime.now(tz=ZoneInfo("Europe/London")).strftime("%H:%M")
                market_hours = {-1: open, 0: now, 1: close}

                # ic(market_hours)
        else:
            market_hours = {-1: "Start", 0: "Live", 1: "End"}
            basis_string = ""

        return market_hours, basis_string

    @app.callback(
        Output("risk-data-store", "data"),
        Output("risk-data-payload-store", "data"),
        Input("generate-risk-button", "n_clicks"),
        State("risk-portfolio-dropdown", "value"),
        State("risk-product-dropdown", "value"),
        State("shock-size-input", "placeholder"),
        State("shock-size-input", "value"),
        State("shock-number-input", "placeholder"),
        State("shock-number-input", "value"),
        State("time_of_day_slider", "value"),
        State("risk-matrix-eval-date", "start_date"),
        State("risk-matrix-eval-date", "end_date"),
        prevent_initial_call=True,
    )
    def send_query_to_risk_engine(
        generate,
        portfolios,
        product,
        p_shock_size,
        shock_size,
        p_shock_number,
        shock_number,
        time_of_day,
        start_date,
        end_date,
    ):
        if not shock_size:
            shock_size = p_shock_size
        if not shock_number:
            shock_number = p_shock_number
        if not portfolios:
            portfolios = []

        ic(start_date)
        ic(end_date)

        # turn start_date and end_date into time zone aware datetime objects
        start_dt = datetime.fromisoformat(start_date).replace(tzinfo=ZoneInfo("UTC"))
        end_dt = datetime.fromisoformat(end_date).replace(tzinfo=ZoneInfo("UTC"))

        # Generate the range of dates
        date_range = []
        current_dt = start_dt
        while current_dt <= end_dt:
            date_range.append(current_dt.isoformat())
            current_dt += dt.timedelta(days=1)

        # date_range = [start_date, end_date]
        # aware_datetimes = [
        #     datetime.fromisoformat(dt_str).replace(tzinfo=ZoneInfo("UTC")).isoformat()
        #     for dt_str in date_range
        # ]

        price_shocks = [i * shock_size for i in range(-shock_number, shock_number + 1)]

        # build query
        url = f"http://{new_risk_api}/v1/generate_matrix"

        payload = {
            "filters": {
                "portfolio_ids": portfolios,
                "product_symbol": product,
            },
            "matrix": {
                "eval_times_utc": date_range,
                "price_shocks": price_shocks,
            },
            "pre_sum": True,
        }
        ic(payload)

        # get
        try:
            response = requests.get(url, json=payload)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(
                "Failed to get data from risk engine: %s with following payload: \n %s",
                e,
                payload,
            )
            return no_update, no_update

        return data, payload

    @app.callback(
        Output("risk-matrix-single-greek-table", "children"),
        Output("risk-matrix-single-date-table", "children"),
        Input("risk-data-store", "data"),
        Input("risk-matrix-greek-dropdown", "value"),
        Input("risk-data-payload-store", "data"),
        State("risk-matrix-eval-date", "start_date"),
        State("risk-matrix-eval-date", "end_date"),
        State("basis-price-label", "children"),
        prevent_initial_call=True,
    )
    def build_risk_matrices(
        risk_data,
        selected_greek,
        payload,
        start_date,
        end_date,
        basis_string,
    ):
        ic(selected_greek)
        basis = float(basis_string.split(":")[1].strip()) if basis_string else 0

        data = risk_data["data"]
        fields = risk_data["fields"]

        eval_times = payload["matrix"]["eval_times_utc"]
        price_shocks = payload["matrix"]["price_shocks"]

        # turn eval times into strings
        eval_times = [
            datetime.fromisoformat(dt_str).strftime("%Y-%m-%d %H:%M")
            for dt_str in eval_times
        ]

        parsed_data = []
        for i, eval_time in enumerate(eval_times):
            eval_data = {"eval_time": str(eval_time), "price_shocks": []}
            for j, price_shock in enumerate(price_shocks):
                shock_data = {"price_shock": price_shock}
                for k, field in enumerate(fields):
                    shock_data[field] = data[i][j][k]
                eval_data["price_shocks"].append(shock_data)
            parsed_data.append(eval_data)
        # ic(parsed_data)

        single_greek_df = create_greek_table(parsed_data, selected_greek)
        # ic(single_greek_df)

        single_date_df = create_all_greeks_table_one_date(
            parsed_data, date_index=0, fields=fields
        )
        # ic(single_date_df)

        shared_multi_columns = [
            {"name": [str(round((basis + float(i)), 2)), str(i)], "id": str(i)}
            for i in single_date_df.columns
            if i != "greek" and i != "eval_datetime"
        ]

        single_greek_table = dtable.DataTable(
            # id="risk-matrix-single-greek-table",
            data=single_greek_df.to_dict("records"),
            # columns=[{"name": str(i), "id": str(i)} for i in single_greek_df.columns],
            columns=[{"name": ["Basis", "eval_datetime"], "id": "eval_datetime"}]
            + shared_multi_columns,
            style_table={"overflowX": "scroll", "minWidth": "100%"},
            style_data_conditional=discrete_background_color_bins(single_greek_df),
        )

        single_date_table = dtable.DataTable(
            # id="risk-matrix-single-date-table",
            data=single_date_df.to_dict("records"),
            columns=[{"name": ["Basis", "greek"], "id": "greek"}]
            + shared_multi_columns,
            # fixed_columns={'headers': True, 'data': 1},
            style_table={"overflowX": "scroll", "minWidth": "100%"},
        )

        return single_greek_table, single_date_table

    ######################################## OLD RISK MATRIX LOGIC
    # risk matrix heat map
    # inputList = ["basisPrice", "shockSize", "shockMax"]

    # @app.callback(
    #     [Output("{}".format(input), "placeholder") for input in inputList],
    #     # Output("shockSize", "placeholder"),
    #     # Output("shockMax", "placeholder"),
    #     [Output("{}".format(input), "value") for input in inputList],
    #     Input("riskPortfolio", "value"),
    # )
    # def load_data(portfolio):
    #     if portfolio:
    #         productCodes = {
    #             "aluminium": "xlme-lad-usd",
    #             "lead": "xlme-pbd-usd",
    #             "zinc": "xlme-lzh-usd",
    #             "copper": "xlme-lcu-usd",
    #             "nickel": "xlme-lnd-usd",
    #         }
    #         product_symbol = productCodes[portfolio.lower()]
    #         with shared_session() as session:
    #             select_front_month_symbol = (
    #                 sqlalchemy.select(upe_static.Option.symbol)
    #                 .where(upe_static.Option.product_symbol == product_symbol)
    #                 .where(
    #                     upe_static.Option.expiry
    #                     >= (
    #                         datetime.now(tz=ZoneInfo("UTC"))
    #                         + relativedelta.relativedelta(days=1)
    #                     )
    #                 )
    #                 .order_by(
    #                     upe_static.Option.product_symbol.asc(),
    #                     upe_static.Option.expiry.asc(),
    #                 )
    #                 .distinct(upe_static.Option.product_symbol)
    #             )
    #             front_month_symbol = session.execute(
    #                 select_front_month_symbol
    #             ).scalar_one_or_none()
    #             if front_month_symbol is None:
    #                 logger.error(
    #                     "Unable to find front month option symbol for %s, %s",
    #                     portfolio,
    #                     product_symbol,
    #                 )
    #                 return 0.0, 0.0, 0.0, "", "", ""
    #         month = conn.get(front_month_symbol)
    #         option_data = pd.DataFrame(orjson.loads(month))

    #         atm = float(option_data.iloc[0]["underlying_prices"])

    #         # tom's preferred placeholders
    #         tomsPlaceholders = {
    #             "aluminium": 20,
    #             "lead": 20,
    #             "zinc": 20,
    #             "copper": 50,
    #             "nickel": 200,
    #         }

    #         basis = round(atm - option_data.iloc[0]["spread"], 0)
    #         shockSize = tomsPlaceholders[portfolio]

    #         # calc shock max as 25% of basis rounded to nearest shock size
    #         shockMax = (round((basis / 4) / shockSize)) * shockSize

    #         return basis, shockSize, shockMax, "", "", ""

    # # populate data
    # @app.callback(
    #     Output("riskData", "data"),
    #     Input("riskMatrix-button", "n_clicks"),
    #     [
    #         State("riskPortfolio", "value"),
    #         # State("riskType", "value"),
    #         State("basisPrice", "placeholder"),
    #         State("basisPrice", "value"),
    #         State("shockSize", "placeholder"),
    #         State("shockSize", "value"),
    #         State("shockMax", "placeholder"),
    #         State("shockMax", "value"),
    #         State("timeStepSize", "placeholder"),
    #         State("timeStepSize", "value"),
    #         State("timeMax", "placeholder"),
    #         State("timeMax", "value"),
    #         State("evalDate", "date"),
    #         # State("abs/rel", "value"),
    #     ],
    #     prevent_initial_call=True,
    # )
    # def load_data(
    #     n_clicks,
    #     portfolio,
    #     basisPriceP,
    #     basisPrice,
    #     shockSizeP,
    #     shockSize,
    #     shockMaxP,
    #     shockMax,
    #     timeStepSizeP,
    #     timeStepSize,
    #     timeMaxP,
    #     timeMax,
    #     evalDate,
    # ):
    #     # placeholder check
    #     if not shockSize:
    #         shockSize = shockSizeP
    #     if not shockMax:
    #         shockMax = shockMaxP
    #     if not timeStepSize:
    #         timeStepSize = timeStepSizeP
    #     if not timeMax:
    #         timeMax = timeMaxP
    #     if not basisPrice:
    #         basisPrice = basisPriceP

    #     evalDate = evalDate.split("T")[0]
    #     # find days offset
    #     if dt.datetime.strptime(evalDate, "%Y-%m-%d") < dt.datetime.today():
    #         days_offset = 0
    #     else:
    #         days_offset = abs(
    #             dt.datetime.today().date()
    #             - dt.datetime.strptime(evalDate, "%Y-%m-%d").date()
    #         ).days

    #     if portfolio and n_clicks > 0:
    #         try:
    #             r = requests.get(
    #                 f"http://{riskAPi}/generate/{portfolio}",
    #                 params={
    #                     "basis_price": str(int(basisPrice)),
    #                     "shock_max": str(int(shockMax)),
    #                     "shock_step": str(int(shockSize)),
    #                     "from_today_offset_days": str(int(days_offset)),
    #                     "time_max": str(int(timeMax)),
    #                     "time_step": str(int(timeStepSize)),
    #                 },
    #             )
    #             data = json.loads(r.text)
    #             return data
    #         except Exception:
    #             logger.exception("error loading data")
    #             return no_update

    # # risk matrix heat map
    # @app.callback(
    #     Output("riskMatrix", "data"),
    #     Output("riskMatrix", "style_data_conditional"),
    #     Input("riskData", "data"),
    #     Input("greeks", "value"),
    #     prevent_initial_call=True,
    # )
    # def heat_map(data, greek):
    #     if data and greek:
    #         df = pd.DataFrame(data)
    #         df = df.applymap(lambda x: x.get(greek))

    #         df = df.swapaxes("index", "columns")
    #         df = df.reset_index()

    #         # round figures for display
    #         if greek == "gamma":
    #             df = df.round(4)
    #         else:
    #             df = df.round(1)

    #         styles = discrete_background_color_bins(df)
    #         data = df.to_dict("records")

    #         return data, styles
    #     else:
    #         return no_update, no_update

    # # second figure
    # @app.callback(
    #     Output("greeksTable", "data"),
    #     Input("riskData", "data"),
    #     prevent_initial_call=True,
    # )
    # def greeksTable(data):
    #     if data:
    #         df = pd.DataFrame(data)

    #         # select today's greeks and transpose
    #         today = df.iloc[:, 0]

    #         df = df.swapaxes("index", "columns")
    #         df = pd.DataFrame(today.to_dict())

    #         # round figures for display
    #         df.iloc[0] = df.iloc[0].round(0)
    #         df.iloc[1] = df.iloc[1].round(0)
    #         df.iloc[2] = df.iloc[2].round(3)  # gamma
    #         df.iloc[3] = df.iloc[3].round(0)
    #         df.iloc[4] = df.iloc[4].round(0)

    #         df = df.reset_index()
    #         df.columns = ["greeks"] + list(df.columns[1:])

    #         data = df.to_dict("records")

    #         return data
    #     else:
    #         return no_update
