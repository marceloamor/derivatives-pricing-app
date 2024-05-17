"""
Homepage displaying portfolio over view and systems status
"""

import json
import os
import traceback
from datetime import date, timedelta
from datetime import datetime as datetime

import dash_bootstrap_components as dbc
import numpy as np
import orjson
import pandas as pd
from dash import dash_table as dtable
from dash import dcc, html, no_update
from dash.dependencies import Input, Output, State
from data_connections import conn
from parts import multipliers, topMenu

product_names = {
    "xlme-lad-usd": "Aluminium",
    "xlme-lcu-usd": "Copper",
    "xlme-pbd-usd": "Lead",
    "xlme-lnd-usd": "Nickel",
    "xlme-lzh-usd": "Zinc",
    "xext-ebm-eur": "Milling Wheat",
    "xice-kc-usd": "Coffee C",
    "xice-sb-usd": "Sugar No. 11",
    "xice-rc-usd": "Robusta Coffee",
}


columns = [
    {"name": "Product", "id": "product"},
    {"name": "Delta", "id": "total_deltas"},
    {"name": "Full Delta", "id": "total_skew_deltas"},
    {"name": "Vega", "id": "total_vegas"},
    {"name": "Theta", "id": "total_thetas"},
    {"name": "Gamma", "id": "total_gammas"},
    {"name": "Full Gamma", "id": "total_skew_gammas"},
    {"name": "Delta Decay", "id": "total_delta_decays"},
    {"name": "Vega Decay", "id": "total_vega_decays"},
    {"name": "Gamma Decay", "id": "total_gamma_decays"},
    {"name": "Gamma Breakeven", "id": "total_gammaBreakEven"},
]
USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "false").lower() in [
    "t",
    "y",
    "true",
    "yes",
]
dev_key_redis_append = "" if not USE_DEV_KEYS else ":dev"

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

ice_totalsTable = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="ice_totals",
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
                dbc.Col(
                    [
                        dbc.Badge(
                            "OEv4",
                            id="v2:gli:1",
                            pill=True,
                            color="success",
                            className="ms-1",
                        )
                    ]
                ),
                dbc.Col(
                    [
                        dbc.Badge(
                            "PEv4",
                            id="pos-eng-v4",
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
    "v2:gli:1",
    "pos-eng-v4",
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

ice_content = dbc.Card(dbc.CardBody([ice_totalsTable], className="mt-3"))

tabs = dbc.Tabs(
    [
        dbc.Tab(lme_content, label="LME General"),
        dbc.Tab(ext_content, label="Euronext General"),
        dbc.Tab(ice_content, label="ICE General"),
        # dbc.Tab(lme_content_old, label="LME Legacy"),
        # dbc.Tab(ext_content_old, label="Euronext Legacy"),
    ]
)

# basic layout
layout = html.Div(
    [
        dcc.Interval(
            id="live-update",
            interval=1 * 1000,
            n_intervals=0,
        ),  # in milliseconds
        dcc.Interval(
            id="live-update2",
            interval=120 * 1000,
            n_intervals=0,  # in milliseconds
        ),
        topMenu("Home"),
        html.Div(
            [
                html.Div([jumbotron]),
                tabs,
                badges,
                colors,
                audios,
            ],
            className="mx-3 my-3",
        ),
    ]
)


# initialise callbacks when generated from app
def initialise_callbacks(app):
    @app.callback(
        [
            Output("lme_totals", "data"),
            Output("ext_totals", "data"),
            Output("ice_totals", "data"),
        ],
        [Input("live-update", "n_intervals")],
    )
    def update_greeks(interval):
        try:
            # new version:
            # pull from new redis key:
            df = conn.get("pos-eng:greek-positions" + dev_key_redis_append).decode(
                "utf-8"
            )
            # turn into pandas df
            df = pd.DataFrame(orjson.loads(df))

            df = df.loc[df["portfolio_id"].isin((1, 3, 5))]

            # create product column from instrument_symbol
            df["product_symbol"] = df["instrument_symbol"].str.split(" ").str[0]

            # group by product and sum
            df = df.groupby("product_symbol").sum(numeric_only=True)

            # re index
            df["product_symbol"] = df.index

            # calc gamma breakeven
            df["multiplier"] = df.loc[:, "product_symbol"].map(multipliers)
            df["total_gammaBreakEven"] = 0.0

            valid_befg_df = df.loc[
                (df["total_skew_gammas"] * df["total_thetas"] < 0.0)
                & (df["total_skew_gammas"].abs() > 1e-6),
                :,
            ]

            df.loc[
                (df["total_skew_gammas"] * df["total_thetas"] < 0.0)
                & (df["total_skew_gammas"].abs() > 1e-6),
                "total_gammaBreakEven",
            ] = np.sqrt(
                -2
                * valid_befg_df["total_thetas"]
                / (valid_befg_df["multiplier"] * valid_befg_df["total_skew_gammas"])
            )

            # split df into lme and ext by first 4 letters
            df["product"] = df["product_symbol"].map(product_names)
            lme_df = df[df["product_symbol"].str.contains("xlme")]
            ext_df = df[df["product_symbol"].str.contains("xext")]
            ice_df = df[df["product_symbol"].str.contains("xice")]

            # to round:
            decimals_dict = {
                "total_deltas": 1,
                "total_skew_deltas": 1,
                "total_vegas": 0,
                "total_thetas": 0,
                "total_gammas": 3,
                "total_skew_gammas": 3,
                "total_delta_decays": 1,
                "total_vega_decays": 0,
                "total_gamma_decays": 3,
                "total_gammaBreakEven": 3,
            }

            # deltas : 1
            # vegas and thetas: 0
            # gammas: 3

            # round and send as dict to dash datatable
            return (
                lme_df.round(decimals=decimals_dict).to_dict("records"),
                ext_df.round(decimals=decimals_dict).to_dict("records"),
                ice_df.round(decimals=decimals_dict).to_dict("records"),
            )

        except Exception:
            print(traceback.format_exc())
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
                "v2:gli:1",
                "pos-eng-v4",
            ]:
                update_time = conn.get(f"{file}:health{dev_key_redis_append}")

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

            elif file in ["clo", "inr", "exr"]:
                # get current date
                update_time = conn.get("{}_update".format(file.upper()))
                if update_time:
                    # update_time = json.loads(update_time)
                    update_time = update_time.decode("utf-8")
                    try:
                        if update_time.split(" ")[-1] == "00:00:00":
                            update_time = datetime.strptime(
                                str(update_time), "%m/%d/%Y, %H:%M:%S"
                            )
                        else:
                            update_time = datetime.strptime(str(update_time), "%Y%m%d")
                        # time data '12/08/2023, 00:00:00' does not match format '%Y%m%d
                    except ValueError:
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
                    except ValueError:
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
