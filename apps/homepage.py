"""
Homepage displaying portfolio over view and systems status
"""

from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table as dtable
from datetime import datetime as datetime
from datetime import date
from datetime import timedelta
from dash import no_update
import json, pickle

from parts import topMenu, pullPortfolioGreeks
from data_connections import conn

columns = [
    {"name": "Portfolio", "id": "portfolio"},
    {"name": "Delta", "id": "total_delta"},
    {"name": "Full Delta", "id": "total_fullDelta"},
    {"name": "Vega", "id": "total_vega"},
    {"name": "Theta", "id": "total_theta"},
    {"name": "Gamma", "id": "total_gamma"},
    {"name": "Delta Decay", "id": "total_deltaDecay"},
    {"name": "Vega Decay", "id": "total_vegaDecay"},
    {"name": "Gamma Decay", "id": "total_gammaDecay"},
]

jumbotron = dbc.Jumbotron(
    [
        html.H1("Georgia", className="display-3"),
        html.P(
            "Welcome to Georgia your specialised LME " "risk and pricing system.",
            className="lead",
        ),
        html.Hr(className="my-2"),
        html.P("Lets get trading!!"),
        html.P(dbc.Button("Learn more", color="primary"), className="lead"),
    ]
)

totalsTable = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="totals",
                    columns=columns,
                    data=[{}],
                    style_data_conditional=[
                        {
                            "if": {"row_index": "odd"},
                            "backgroundColor": "rgb(248, 248, 248)",
                        }
                    ],
                )
            ]
        )
    ]
)

badges = dbc.Row(
    [
        dbc.Col(
            [dbc.Badge("Vols", id="vols", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("FCP", id="fcp", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("INR", id="inr", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("EXR", id="exr", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("NAP", id="nap", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("SMP", id="smp", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("TCP", id="tcp", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("CLO", id="clo", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("ACP", id="acp", pill=True, color="success", className="ms-1")]
        ),
        dbc.Col(
            [dbc.Badge("SCH", id="sch", pill=True, color="success", className="ms-1")]
        ),
    ]
)

files = ["vols", "fcp", "inr", "exr", "nap", "smp", "tcp", "clo", "acp", "sch"]

# basic layout
layout = html.Div(
    [
        dcc.Interval(
            id="live-update", interval=1 * 1000, n_intervals=0  # in milliseconds
        ),
        dcc.Interval(
            id="live-update2", interval=360 * 1000, n_intervals=0  # in milliseconds
        ),
        topMenu("Home"),
        html.Div([jumbotron]),
        totalsTable,
        badges,
    ]
)

# initialise callbacks when generated from app
def initialise_callbacks(app):

    # pull totals for
    @app.callback(Output("totals", "data"), [Input("live-update", "n_intervals")])
    def update_greeks(interval):

        try:
            # pull greeks from Redis
            dff = pullPortfolioGreeks()

            # sum by portfolio
            dff = dff.groupby("portfolio").sum()

            dff["portfolio"] = dff.index

            # round and send as dict to dash datatable
            return dff.round(3).to_dict("records")

        except Exception as e:

            return no_update

    # change badge button color depending on age of files
    @app.callback(
        [Output("{}".format(file), "color") for file in files],
        [Input("live-update2", "n_intervals")],
    )
    def update_greeks(interval):

        # default to list of "danger"
        color_list = ["danger" for i in files]

        i = 0
        for file in files:
            if file == "vols":
                # pull date from lme_vols
                vols = conn.get("lme_vols")
                vols = pickle.loads(vols)

                vols_date = vols.iloc[0]["Date"]
                update_time = datetime.strptime(str(vols_date), "%d%b%y")

                # getting difference taking account of weekend
                if date.today().weekday() == 0:
                    diff = 3
                elif date.today().weekday() == 6:
                    diff = 2
                else:
                    diff = 1

                # compare to yesterday to see if old
                yesterday = date.today() - timedelta(days=diff)

                if update_time.date() == yesterday:
                    color_list[i] = "success"
                else:
                    color_list[i] = "danger"
            else:
                # get current date
                update_time = conn.get("{}_update".format(file.upper()))
                if update_time:
                    update_time = json.loads(update_time)
                    update_time = datetime.strptime(str(update_time), "%Y%m%d")

                    # getting difference
                    if date.today().weekday() == 0:
                        diff = 3
                    elif date.today().weekday() == 6:
                        diff = 2
                    else:
                        diff = 1

                    # compare to yesterday to see if old
                    yesterday = date.today() - timedelta(days=diff)
                    if update_time.date() == yesterday:
                        color_list[i] = "success"
                    else:
                        color_list[i] = "danger"
                else:
                    color_list[i] = "danger"

            i = i + 1
        return color_list
