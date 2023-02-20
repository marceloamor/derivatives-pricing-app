from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
from datetime import datetime as dt
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from dash import dash_table as dtable

from parts import pullRates, topMenu

interval = 150

graph = html.Div(
    [dcc.Graph(id="usd"), dcc.Interval(id="live-update", interval=interval)],
    className="row",
)

layout = html.Div(
    [topMenu("Rates"), html.Div([dcc.Link("Home", href="/")], className="row"), 
     graph,
    dcc.Interval(
            id="live-update", interval=200, n_intervals=0  # in milliseconds
        ),
    html.Div(id="output-rates-table"),]
)


def initialise_callbacks(app):
    # update graphs on data update
    @app.callback(
        Output(component_id="usd", component_property="figure"),
        #Output(component_id="output-rates-table", component_property="children"),
        [Input("live-update", "interval")],
    )
    def load_param_graph(interval):
        
        rates = pullRates("USD")
        # sort data on date and adjust current dataframe
        rates.sort_index(inplace=True)

        # find the axis values
        rates.index = pd.to_datetime(rates.index)
        index = rates.index
        forwardDate = list(index)
        price = np.array(rates["Interest Rate"])
        # build scatter graph pd.to_datetime([dates)
        figure = go.Figure(
            data=[
                go.Scatter(
                    x=forwardDate,
                    y=price,
                    mode="lines",
                    hoveron="points",
                    line=dict(
                        color=("rgb(22, 96, 167)"),
                        width=2,
                    ),
                )
            ],
            layout=go.Layout(title="Rates", showlegend=False),
        )
        return figure

        # print("!!!!!")
        # rates = pullRates("USD")
        # # sort data on date and adjust current dataframe

        # print(rates)
        # # rates.sort_index(inplace=True)

        # rates_table = dtable.DataTable(
        #     data=rates.to_dict("records"),
        #     columns=[{"name": col_name, "id": col_name} for col_name in rates.columns],
        # )
        # print(rates_table)

        # # find the axis values
        # rates.index = pd.to_datetime(rates.index)
        # rates_table = dtable.DataTable(
        #         data=rates.to_dict("records"),
        #         columns=[
        #             {"name": col_name, "id": col_name} for col_name in rates.columns
        #         ],
        #     )
        # index = rates.index
        # forwardDate = list(index)
        # price = np.array(rates["Interest Rate"])
        # # build scatter graph pd.to_datetime([dates)
        # figure = go.Figure(
        #     data=[
        #         go.Scatter(
        #             x=forwardDate,
        #             y=price,
        #             mode="lines",
        #             hoveron="points",
        #             line=dict(
        #                 color=("rgb(22, 96, 167)"),
        #                 width=2,
        #             ),
        #         )
        #     ],
        #     layout=go.Layout(title="Rates", showlegend=False),
        # )
        # return rates_table
