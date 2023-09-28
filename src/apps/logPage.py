from parts import topMenu

from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html
import pandas as pd


interval = str(2000)

COLORS = [
    {"background": "#fef0d9", "text": "rgb(30, 30, 30)"},
    {"background": "#fdcc8a", "text": "rgb(30, 30, 30)"},
    {"background": "#fc8d59", "text": "rgb(30, 30, 30)"},
    {"background": "#d7301f", "text": "rgb(30, 30, 30)"},
]

columns = [{"name": "Warning", "id": "0"}]

# trades table layout
log_table = dbc.Col(
    [
        dtable.DataTable(
            id="logsLines",
            columns=columns,
            data=[{}],
            fixed_rows=[{"headers": True, "data": 0}],
        )
    ]
)

layout = html.Div(
    [
        topMenu("Logs"),
        dcc.Interval(id="live-update", interval=interval),
        dbc.Row([log_table]),
    ]
)


def initialise_callbacks(app):
    # pull logs
    @app.callback(Output("logsLines", "data"), [Input("live-update", "interval")])
    def update_greeks(interval):
        # pull logs from file set engine = python as C does not reconise some of the warnings
        df = pd.read_csv(
            r"\\BENDER\Users$\gareth.upe\Desktop\LME\release\LME.log",
            header=None,
            sep="delimiter",
            engine="python",
        )
        # reorder so nesest at top
        if not df.empty:
            df = df[::-1]
            return df.to_dict("records")
