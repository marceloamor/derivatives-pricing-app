from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_daq as daq
import pandas as pd
import dash_table as dtable
from dash import no_update
import json, colorlover

from data_connections import conn
from parts import topMenu, onLoadPortFolio, loadStaticData


def strikeRisk(portfolio, riskType, relAbs, zeros=False):
    # pull list of porducts from static data
    portfolioGreeks = conn.get("greekpositions")
    if portfolioGreeks:
        portfolioGreeks = json.loads(portfolioGreeks)
        portfolioGreeks = pd.DataFrame.from_dict(portfolioGreeks)
        products = portfolioGreeks[portfolioGreeks.portfolio == portfolio][
            "underlying"
        ].unique()

        # setup greeks and products bucket to collect data
        greeks = []
        dfData = []

        # if zeros build strikes from product
        if zeros:
            static = loadStaticData()
            static.set_index("underlying", inplace=True)
            max_strike = max(static.loc[static["portfolio"] == portfolio, "strike_max"])
            min_strike = min(static.loc[static["portfolio"] == portfolio, "strike_min"])
            strike_step = min(
                static.loc[static["portfolio"] == portfolio, "strike_step"]
            )

            allStrikes = range(int(min_strike), int(max_strike), int(strike_step))

        if relAbs == "strike":
            # for each product collect greek per strike
            for product in products:
                data = portfolioGreeks[portfolioGreeks.underlying == product]
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
                            id="heatMapTable",
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

layout = html.Div(
    [
        topMenu("Strike Risk"),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="strike-portfolio-selector",
                            value="copper",
                            options=onLoadPortFolio(),
                        )
                    ],
                    width=3,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="strike-risk-selector",
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
                    ],
                    width=3,
                ),
                dbc.Col(
                    [
                        dcc.Dropdown(
                            id="relAbs",
                            value="strike",
                            options=[
                                {"label": "Strike", "value": "strike"},
                                {"label": "Bucket", "value": "bucket"},
                            ],
                        )
                    ],
                    width=3,
                ),
                dbc.Col(["Zero Strikes"]),
                dbc.Col([daq.BooleanSwitch(id="zeros", on=False)]),
            ]
        ),
        heatMap,
    ]
)


def initialise_callbacks(app):
    @app.callback(
        Output("heatMapTable", "data"),
        Output("heatMapTable", "columns"),
        Output("heatMapTable", "style_data_conditional"),
        [
            Input("strike-portfolio-selector", "value"),
            Input("strike-risk-selector", "value"),
            Input("relAbs", "value"),
            Input("zeros", "on"),
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

            data = df.to_dict("records")

            styles = discrete_background_color_bins(df)

            return data, columns, styles
