# Strike risk using new static data etc to pull in position. to be used till the LME is moved to the new system
# CREATED: gareth 4/4/2023

from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
import dash_bootstrap_components as dbc
import dash_daq as daq
import pandas as pd
from dash import dash_table as dtable
from dash import no_update
import json, colorlover

from data_connections import conn
from parts import topMenu, onLoadPortFolio, loadStaticData
from sql_utils import strike_range, productList

from datetime import datetime


def strikeRisk(portfolio, riskType, relAbs, zeros=False):
    # pull list of porducts from static data
    data = conn.get("greekpositions_xext:dev")
    if data != None:
        portfolioGreeks = pd.read_json(data)

        # might be a better and more efficent way to do this using the greek-pos dataframe
        # solution was whipped up in a few minutes sooo...
        products = sorted(
            portfolioGreeks[
                (portfolioGreeks.portfolio == portfolio) & (portfolioGreeks.strike)
            ]["contract_symbol"].unique(),
            key=lambda option_symbol: datetime.strptime(
                option_symbol.split(" ")[2], r"%y-%m-%d"
            ),
        )

        # setup greeks and products bucket to collect data
        greeks = []
        dfData = []

        allStrikes = strike_range(portfolio)

        if relAbs == "strike":
            # for each product collect greek per strike
            for product in products:
                data = portfolioGreeks[portfolioGreeks.contract_symbol == product]
                strikegreeks = []

                if zeros:
                    strikes = allStrikes
                else:
                    strikes = data["strike"]
                # go over strikes and uppack greeks

                strikeRisk = {}
                for strike in strikes:
                    # pull product mult to convert greeks later
                    if strike in data["strike"].astype(int).tolist():
                        risk = data.loc[data.strike == strike][riskType].sum()
                    else:
                        risk = 0

                    strikegreeks.append(risk)
                    strikeRisk[round(strike)] = risk
                greeks.append(strikegreeks)
                dfData.append(strikeRisk)

            df = pd.DataFrame(dfData, index=products)

            # if zeros then reverse order so both in same order
            if not zeros:
                df = df.iloc[:, ::-1]
            df.fillna(0, inplace=True)

            return df.round(3), products
    else:
        return None, None


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
    value="quanitity",
    options=[
        {"label": "Vega", "value": "total_vega"},
        {"label": "Gamma", "value": "total_gamma"},
        {"label": "Delta", "value": "total_delta"},
        {"label": "Theta", "value": "total_theta"},
        {"label": "Gamma", "value": "total_gamma"},
        {"label": "Gamma Decay", "value": "total_gammaDecay"},
        {"label": "Vega Decay", "value": "total_vegaDecay"},
        {"label": "Delta Decay", "value": "total_deltaDecay"},
        {"label": "Full Delta", "value": "total_fullDelta"},
        {"label": "Position", "value": "quanitity"},
    ],
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
            Input("strike-portfolio-selectorNew", "value"),
            Input("strike-risk-selectorNew", "value"),
            Input("relAbsNew", "value"),
            Input("zerosNew", "on"),
        ],
    )
    def update_greeks(portfolio, riskType, relAbs, zeros):
        # pull dataframe and products
        df, products = strikeRisk(portfolio, riskType, relAbs, zeros=zeros)

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
