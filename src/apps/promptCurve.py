from parts import topMenu, onLoadPortFolio, portfolioToProduct
from data_connections import conn

from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
import plotly.graph_objs as go
from dash import dcc, html
import pandas as pd
import numpy as np


# 1 second interval
interval = 1000 * 1


def generate_table(dataframe, max_rows=75):
    """Given dataframe, return template generated using Dash components"""
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns[::-1]])] +
        # Body
        [
            html.Tr(
                [html.Td(dataframe.iloc[i][col]) for col in dataframe.columns[::-1]]
            )
            for i in range(min(len(dataframe), max_rows))
        ],
        style={"background": "#fff"},
    )


graph = dbc.Col(
    [dcc.Graph(id="prompt-curve"), dcc.Interval(id="live-update", interval=interval)]
)

tables = dbc.Col(
    [
        dtable.DataTable(
            id="prompt-table",
            # columns=columns,
            data=[{}],
            fixed_rows={"headers": True},
        )
    ],
    width=12,
)
options = dbc.Col(
    [dcc.Dropdown(id="portfolio-selector", value="copper", options=onLoadPortFolio())],
    width=4,
)

submit = dbc.Col([html.Button("Submit Spreads", id="SumbitSpreads")], width=4)

layout = html.Div(
    [
        topMenu("Prompt Curve"),
        dbc.Row([options]),
        dbc.Row([graph]),
        # table holder
        dbc.Row(
            [tables],
        ),
        # table option holder
        dbc.Row([submit]),
    ]
)


def initialise_callbacks(app):
    # update graphs on data update
    @app.callback(Output("prompt-curve", "figure"), [Input("prompt-table", "data")])
    def load_prompt_graph(rates):
        # pull prompt curve
        rates = pd.DataFrame.from_dict(rates).head(100)

        # return blank is no data
        if rates.empty:
            return {}

        # find the axis values
        index = pd.to_datetime(rates["third_wed"], format="%Y-%m-%d")
        forwardDate = list(index)
        # price = np.array(rates["price"])
        # trace1 = go.Scatter(
        #         x=forwardDate,
        #         y=price,
        #         mode="lines",
        #         hoveron="points",
        #         name="Price",
        #         visible="legendonly",
        #         line=dict(
        #             color=("rgb(22, 96, 167)"),
        #             width=2,
        #         ),
        #     )

        position = np.array(rates["total_delta_futures"])
        trace2 = go.Bar(
            x=forwardDate, y=position, yaxis="y2", name="Futures", visible="legendonly"
        )

        total = np.array(rates["total_fullDelta"])
        trace4 = go.Bar(x=forwardDate, y=total, yaxis="y2", name="Total")

        cumlative = np.array(rates["cumlative"])
        trace3 = go.Scatter(
            x=forwardDate,
            y=cumlative,
            mode="lines",
            yaxis="y2",
            hoveron="points",
            name="Cumlative",
            line=dict(
                color=("rgb(220, 244, 66)"),
                width=2,
            ),
        )
        optionDelta = np.array(rates["total_fullDelta_options"])
        trace5 = go.Bar(
            x=forwardDate,
            y=optionDelta,
            yaxis="y2",
            name="Options Delta",
            visible="legendonly",
        )

        figure = go.Figure(
            data=[trace2, trace3, trace4, trace5],
            layout=go.Layout(
                title="Prompt Curve",
                yaxis=dict(title="Price"),
                yaxis2=dict(
                    title="Position",
                    titlefont=dict(color="rgb(148, 103, 189)"),
                    tickfont=dict(color="rgb(148, 103, 189)"),
                    overlaying="y",
                    side="right",
                ),
                xaxis=dict(title="Date"),
            ),
        )
        return figure

    # update table on data update
    @app.callback(
        [Output("prompt-table", "data"), Output("prompt-table", "columns")],
        [Input("portfolio-selector", "value")],
    )
    def load_prompt_table(portfolio):
        pos_json = conn.get("greekpositions")
        pos = pd.read_json(pos_json)

        # product from portfolio
        product = portfolioToProduct(portfolio)

        # filter for product
        pos = pos[pos["product"].str[:3] == product][
            ["product", "third_wed", "total_delta", "total_fullDelta", "cop"]
        ]

        columns = [
            {"name": "Expiry", "id": "third_wed"},
            {"name": "Total", "id": "total_fullDelta"},
            {"name": "Futures", "id": "total_delta_futures"},
            {"name": "Options", "id": "total_fullDelta_options"},
            {"name": "Cumlative", "id": "cumlative"},
        ]

        # if greeks empty
        if pos.empty:
            return [{}], []

        # filter out futures and options
        options = pos[pos["cop"].isin(["c", "p"])][
            ["third_wed", "total_delta", "total_fullDelta"]
        ]
        futures = pos[~pos["cop"].isin(["c", "p"])][
            ["third_wed", "total_delta", "total_fullDelta"]
        ]

        # group by third wed
        options = options.groupby("third_wed").sum()
        futures = futures.groupby("third_wed").sum()
        pos = pos.groupby("third_wed").sum()

        options = options.rename(
            {"total_fullDelta": "total_fullDelta_options"}, axis="columns"
        )
        futures = futures.rename({"total_delta": "total_delta_futures"}, axis="columns")

        pos = pd.concat([pos, options["total_fullDelta_options"]], axis=1, sort=False)
        pos = pd.concat([pos, futures["total_delta_futures"]], axis=1, sort=False)

        # add cumaltive column
        pos["cumlative"] = pos["total_fullDelta"].cumsum()

        # round and reset index
        pos = pos.round(2).reset_index()

        columns = [
            {"name": "Expiry", "id": "third_wed"},
            {"name": "Total", "id": "total_fullDelta"},
            {"name": "Futures", "id": "total_delta_futures"},
            {"name": "Options", "id": "total_fullDelta_options"},
            {"name": "Cumlative", "id": "cumlative"},
        ]

        return pos.to_dict("records"), columns
