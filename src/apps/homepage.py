"""
Homepage displaying portfolio over view and systems status
"""

import traceback
from dash.dependencies import Input, Output, State
from dash import dcc, html
from dash import dcc
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from datetime import datetime as datetime
from datetime import date
from datetime import timedelta
from dash import no_update
import json, pickle
import pandas as pd

from parts import topMenu, pullPortfolioGreeks
from data_connections import conn

columns = [
    {"name": "Portfolio", "id": "portfolio"},
    {"name": "Delta", "id": "total_delta"},
    {"name": "Full Delta", "id": "total_fullDelta"},
    {"name": "Vega", "id": "total_vega"},
    {"name": "Theta", "id": "total_theta"},
    {"name": "Gamma", "id": "total_gamma"},
    {"name": "Full Gamma", "id": "total_fullGamma"},
    {"name": "Delta Decay", "id": "total_deltaDecay"},
    {"name": "Vega Decay", "id": "total_vegaDecay"},
    {"name": "Gamma Decay", "id": "total_gammaDecay"},
]

jumbotron = dbc.Container(
    [
        html.H1("Georgia", className="display-3"),
        html.P(
            "Welcome to Georgia your specialised options " "risk and pricing system.",
            className="lead",
        ),
        html.Hr(className="my-2"),
        html.P("Lets get trading!!"),
        html.P(dbc.Button("Learn more", color="primary"), className="lead"),
    ]
)

lme_totalsTable = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="lme_totals",
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


ext_totalsTable = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="ext_totals",
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


badges = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Badge(
                            "Vols",
                            id="vols",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "FCP",
                            id="fcp",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "INR",
                            id="inr",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "EXR",
                            id="exr",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "NAP",
                            id="nap",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "SMP",
                            id="smp",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "TCP",
                            id="tcp",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "CLO",
                            id="clo",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "ACP",
                            id="acp",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "SCH",
                            id="sch",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Badge(
                            "MD", id="md", pill=True, color="success", className="ms-1"
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "Trade",
                            id="tradesub",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "LMEOpEng",
                            id="lme_oe_interface",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "LMEPosEng",
                            id="lme_poseng",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "TTDropcopy",
                            id="tt_fix_dropcopy",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "Sol3PME",
                            id="pme_trade_watcher",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "RJORouter",
                            id="rjo_lme_sftp_router",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
            ]
        ),
    ]
)

files = [
    "vols",
    "fcp",
    "inr",
    "exr",
    "nap",
    "smp",
    "tcp",
    "clo",
    "acp",
    "sch",
    "md",
    "tradesub",
    "lme_oe_interface",
    "lme_poseng",
    "tt_fix_dropcopy",
    "pme_trade_watcher",
    "rjo_lme_sftp_router",
]

colors = dbc.Row([dcc.Store(id=f"{file}_color") for file in files])

audios = dbc.Row([html.Div(id=f"{file}_audio") for file in files])

yoda_death_sound = "/assets/sounds/lego-yoda-death-sound-effect.mp3"

# tabs to seperate portfolio sources

lme_content = dbc.Card(
    dbc.CardBody([lme_totalsTable]),
    className="mt-3",
)

ext_content = dbc.Card(
    dbc.CardBody([ext_totalsTable]),
    className="mt-3",
)

tabs = dbc.Tabs(
    [
        dbc.Tab(lme_content, label="LME"),
        dbc.Tab(ext_content, label="Euronext"),
    ]
)

# basic layout
layout = html.Div(
    [
        dcc.Interval(
            id="live-update", interval=1 * 1000, n_intervals=0  # in milliseconds
        ),
        dcc.Interval(
            id="live-update2", interval=120 * 1000, n_intervals=0  # in milliseconds
        ),
        topMenu("Home"),
        html.Div([jumbotron]),
        tabs,
        badges,
        colors,
        audios,
    ]
)


# initialise callbacks when generated from app
def initialise_callbacks(app):
    # pull totals for lme totalstable
    @app.callback(Output("lme_totals", "data"), [Input("live-update", "n_intervals")])
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

    @app.callback(Output("ext_totals", "data"), [Input("live-update", "n_intervals")])
    def update_greeks(interval):
        try:
            # pull greeks from Redis
            # dff = pullPortfolioGreeks()

            data = conn.get("greekpositions_xext:dev")
            if data != None:
                dff = pd.read_json(data)

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

            elif file in [
                "md",
                "tradesub",
                "lme_oe_interface",
                "lme_poseng",
                "tt_fix_dropcopy",
                "pme_trade_watcher",
            ]:
                update_time = conn.get("{}:health".format(file))

                # compare to yesterday to see if old
                time_cutoff = datetime.now() - timedelta(seconds=40)
                if update_time:
                    update_time = datetime.fromtimestamp(json.loads(update_time))
                    if update_time > time_cutoff:
                        color_list[i] = "success"
                else:
                    color_list[i] = "danger"

            elif file in [
                "rjo_lme_sftp_router",
            ]:
                update_time = conn.get("{}:health".format(file))

                # compare to yesterday to see if old
                time_cutoff = datetime.now() - timedelta(seconds=90)
                if update_time:
                    update_time = datetime.fromtimestamp(json.loads(update_time))
                    if update_time > time_cutoff:
                        color_list[i] = "success"
                else:
                    color_list[i] = "danger"

            else:
                # get current date
                update_time = conn.get("{}_update".format(file.upper()))
                if update_time:
                    # update_time = json.loads(update_time)
                    update_time = update_time.decode("utf-8")
                    try:
                        update_time = datetime.strptime(
                            str(update_time), "%m/%d/%Y, %H:%M:%S"
                        )
                    except ValueError as e:
                        print(traceback.format_exc())
                        update_time = datetime.utcfromtimestamp(0.0)

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

    # play alert sound if badge changes color to red
    for file in files:

        @app.callback(
            Output("{}_color".format(file), "data"),
            Output("{}_audio".format(file), "children"),
            Input("{}".format(file), "color"),
            State("live-update2", "n_intervals"),
            State("{}_color".format(file), "data"),
        )
        def badgeSounds(color, interval, stored_color):
            audio = ""
            if interval > 0:
                if color == "danger" and stored_color == "success":
                    audio = html.Audio(src=yoda_death_sound, id="audio", autoPlay=True)
            stored_color = color
            return stored_color, audio
