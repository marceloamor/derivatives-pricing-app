# Strike risk using new static data etc to pull in position. to be used till the LME is moved to the new system
# CREATED: gareth 4/4/2023

import os

import colorlover
import dash_bootstrap_components as dbc
import dash_daq as daq
import orjson
import pandas as pd
from dash import dash_table as dtable
from dash import dcc, html, no_update
from dash.dependencies import Input, Output
from data_connections import conn
from parts import dev_key_redis_append, topMenu
from sql_utils import productList, strike_range

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "true",
    "t",
    "1",
    "y",
    "yes",
]
if USE_DEV_KEYS:
    pass


def strikeRisk_old(portfolio, riskType, relAbs, zeros=False):
    new_data = conn.get("pos-eng:greek-positions" + dev_key_redis_append)
    new_data = pd.DataFrame(orjson.loads(new_data))

    # filter away futures
    new_data = new_data[new_data["contract_type"] != "f"]

    # filter on product
    new_data["split_symbol"] = new_data["instrument_symbol"].str.split(" ")
    new_data["product"] = new_data["instrument_symbol"].str.split(" ").str[0]
    new_data = new_data[new_data["product"].str.startswith(portfolio)]

    # sort df by expiry
    new_data["expiry"] = pd.to_datetime(new_data["expiry_date"])
    new_data = new_data.sort_values(by="expiry_date")

    # unique list of options, sorted by expiry, built from first 3 words of option symbol
    new_data.loc[new_data["contract_type"] == "o", "option_symbol"] = (
        new_data.loc[new_data["contract_type"] == "o", "split_symbol"]
        .str[0:3]
        .str.join(" ")
        .str.cat(
            new_data.loc[new_data["contract_type"] == "o", "split_symbol"]
            .str[-1]
            .str.split("-")
            .str[0],
            sep=" ",
        )
    )
    options_list = new_data["option_symbol"].unique().tolist()
    strikes_list = strike_range(portfolio)

    # setup greeks and products bucket to collect data
    greeks2 = []
    dfData2 = []

    if relAbs == "strike":
        # for each product collect greek per strike
        for option_symbol in options_list:
            data2 = new_data[new_data["option_symbol"] == option_symbol]
            strikegreeks = []

            if zeros:
                strikes = strikes_list
            else:
                strikes = data2["strikes"]
            # go over strikes and uppack greeks

            strikeRisk = {}
            for strike in strikes:
                # pull product mult to convert greeks later
                if strike in data2["strikes"].astype(int).tolist():
                    risk = data2.loc[data2.strikes == strike][riskType].sum()
                else:
                    risk = 0

                strikegreeks.append(risk)
                strikeRisk[round(strike)] = risk
            greeks2.append(strikegreeks)
            dfData2.append(strikeRisk)
        df2 = pd.DataFrame(dfData2, index=options_list)

        # if zeros then reverse order so both in same order
        if not zeros:
            df2 = df2.iloc[:, ::-1]
        df2.fillna(0, inplace=True)

        return df2.round(3), options_list


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


heatMap = dbc.Row(
    [
        dbc.Col(
            [
                dcc.Loading(
                    id="loading-2",
                    type="circle",
                    children=[
                        dtable.DataTable(
                            id="heatMapTableNew",
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

# Dropdowns and labels
productDropdown = dcc.Dropdown(
    id="strike-portfolio-selectorNew",
    value=productList[0]["value"],
    options=productList,
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)


greeksDropdown = dcc.Dropdown(
    id="strike-risk-selectorNew",
    value="net_quantity",
    options=[
        {"label": "Position", "value": "net_quantity"},
        {"label": "Delta", "value": "total_deltas"},
        {"label": "Skew Delta", "value": "total_skew_deltas"},
        {"label": "Vega", "value": "total_vegas"},
        {"label": "Theta", "value": "total_thetas"},
        {"label": "Gamma", "value": "total_gammas"},
        {"label": "Skew Gamma", "value": "total_skew_gammas"},
        {"label": "Delta Decay", "value": "total_delta_decays"},
        {"label": "Vega Decay", "value": "total_vega_decays"},
        {"label": "Gamma Decay", "value": "total_gamma_decays"},
    ],
    clearable=False,
)
greeksLabel = html.Label(
    ["Greeks:"], style={"font-weight": "bold", "text-align": "left"}
)

typeSelector = dcc.Dropdown(
    id="relAbsNew",
    value="strike",
    options=[
        {"label": "Strike", "value": "strike"},
    ],
)
typeLabel = html.Label(
    ["Strike/Bucket:"], style={"font-weight": "bold", "text-align": "left"}
)

zeroStrikesSwitch = daq.BooleanSwitch(id="zerosNew", on=False)
zeroStrikesLabel = html.Label(
    ["Zero Strikes:"], style={"font-weight": "bold", "text-align": "center"}
)


selectors = dbc.Row(
    [
        dbc.Col(
            [productLabel, productDropdown],
            width=3,
        ),
        dbc.Col(
            [greeksLabel, greeksDropdown],
            width=3,
        ),
        dbc.Col(
            [typeLabel, typeSelector],
            width=3,
        ),
        dbc.Col(
            [zeroStrikesLabel, zeroStrikesSwitch],
            width=3,
        ),
    ]
)

layout = html.Div(
    [
        topMenu("Strike Risk"),
        selectors,
        heatMap,
    ]
)


def initialise_callbacks(app):
    @app.callback(
        Output("heatMapTableNew", "data"),
        Output("heatMapTableNew", "columns"),
        Output("heatMapTableNew", "style_data_conditional"),
        [
            Input("strike-portfolio-selectorNew", "value"),  # product
            Input("strike-risk-selectorNew", "value"),  # greek
            Input("relAbsNew", "value"),  # strike / bucket
            Input("zerosNew", "on"),  # zero strikes bool
        ],
    )
    def update_greeks(portfolio, riskType, relAbs, zeros):
        # pull dataframe and products
        df, products = strikeRisk_old(portfolio, riskType, relAbs, zeros=zeros)

        if df.empty:
            return [{}], [], no_update
        else:
            # create columns
            columns = [{"id": "product", "name": "Product"}] + [
                {"id": str(i), "name": str(i)} for i in sorted(df.columns.values)
            ]

            df["product"] = products
            # create data
            df = df.loc[~(df["product"] == "None")]

            # convert column names to strings fo json
            df.columns = df.columns.astype(str)

            # sort based on product name
            df[["first_value", "last_value"]] = df["product"].str.extract(
                r"([ab])?(\d)"
            )
            df = df.sort_values(by=["first_value", "last_value"])
            df.drop(columns=["last_value", "first_value"], inplace=True)

            data = df.to_dict("records")

            styles = discrete_background_color_bins(df)

            return data, columns, styles
