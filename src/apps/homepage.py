"""
Homepage displaying portfolio over view and systems status
"""
from parts import topMenu, pullPortfolioGreeks, multipliers
from data_connections import conn

from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html
from dash import no_update
import pandas as pd
import numpy as np
import orjson

from datetime import datetime as datetime
from datetime import timedelta
from datetime import date
import json, pickle
import traceback

multipliers_new = {
    "xlme-lad-usd": 25,
    "xlme-lcu-usd": 25,
    "xlme-pbd-usd": 25,
    "xlme-lnd-usd": 6,
    "xlme-lzh-usd": 25,
    "xext-ebm-eur": 50,
}

product_names = {
    "xlme-lad-usd": "Aluminium",
    "xlme-lcu-usd": "Copper",
    "xlme-pbd-usd": "Lead",
    "xlme-lnd-usd": "Nickel",
    "xlme-lzh-usd": "Zinc",
    "xext-ebm-eur": "Milling Wheat",
}


columns_old = [
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
    {"name": "Gamma Breakeven", "id": "total_gammaBreakEven"},
]

columns = [
    {"name": "Portfolio", "id": "product"},
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

lme_totalsTable_old = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="lme_totals_old",
                    columns=columns_old,
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


ext_totalsTable_old = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="ext_totals_old",
                    columns=columns_old,
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

lme_content_old = dbc.Card(
    dbc.CardBody([lme_totalsTable_old]),
    className="mt-3",
)

ext_content_old = dbc.Card(
    dbc.CardBody([ext_totalsTable_old]),
    className="mt-3",
)

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
        dbc.Tab(lme_content_old, label="LME_old"),
        dbc.Tab(ext_content_old, label="Euronext_old"),
        dbc.Tab(lme_content, label="LME"),
        dbc.Tab(ext_content, label="Euronext"),
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
    @app.callback(
        Output("lme_totals", "data"),
        Output("ext_totals", "data"),
        [Input("live-update", "n_intervals")],
    )
    def update_greeks(interval):
        try:
            # new version:
            # pull from new redis key:
            df = conn.get("pos-eng:greek-positions:dev").decode("utf-8")
            # turn into pandas df
            df = orjson.loads(df)

            # turn into pandas df
            df = pd.DataFrame(df)

            # create product column from instrument_symbol
            df["product_symbol"] = df["instrument_symbol"].str.split(" ").str[0]

            # group by product and sum
            df = df.groupby("product_symbol").sum(numeric_only=True)

            # re index
            df["product_symbol"] = df.index

            # calc gamma breakeven
            df["multiplier"] = df.loc[:, "product_symbol"].map(multipliers_new)
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
            return lme_df.round(decimals=decimals_dict).to_dict(
                "records"
            ), ext_df.round(decimals=decimals_dict).to_dict("records")

        except Exception as e:
            print(traceback.format_exc())
            return no_update

    # deprecate old tabs and tables
    @app.callback(
        Output("lme_totals_old", "data"), [Input("live-update", "n_intervals")]
    )
    def update_greeks(interval):
        try:
            # pull greeks from Redis
            dff = pullPortfolioGreeks()

            # sum by portfolio
            dff = dff.groupby("portfolio").sum(numeric_only=True)

            dff["portfolio"] = dff.index
            dff["multiplier"] = dff.loc[:, "portfolio"].map(multipliers)
            dff["total_gammaBreakEven"] = 0.0

            valid_befg_df = dff.loc[
                (dff["total_fullGamma"] * dff["total_theta"] < 0.0)
                & (dff["total_fullGamma"].abs() > 1e-6),
                :,
            ]

            dff.loc[
                (dff["total_fullGamma"] * dff["total_theta"] < 0.0)
                & (dff["total_fullGamma"].abs() > 1e-6),
                "total_gammaBreakEven",
            ] = np.sqrt(
                -2
                * valid_befg_df["total_theta"]
                / (valid_befg_df["multiplier"] * valid_befg_df["total_fullGamma"])
            )

            # round and send as dict to dash datatable
            return dff.round(3).to_dict("records")

        except Exception as e:
            print(traceback.format_exc())
            return no_update

    @app.callback(
        Output("ext_totals_old", "data"), [Input("live-update", "n_intervals")]
    )
    def update_greeks(interval):
        try:
            data = conn.get("greekpositions_xext:dev")

            if data != None:
                data = data.decode("utf-8")
                dff = pd.read_json(data)

            # sum by portfolio
            dff = dff.groupby("portfolio").sum(numeric_only=True)

            dff["portfolio"] = dff.index
            dff["multiplier"] = dff.loc[:, "portfolio"].map(multipliers)
            dff["total_gammaBreakEven"] = 0.0

            valid_befg_df = dff.loc[
                (dff["total_fullGamma"] * dff["total_theta"] < 0.0)
                & (dff["total_fullGamma"].abs() > 1e-6),
                :,
            ]

            dff.loc[
                (dff["total_fullGamma"] * dff["total_theta"] < 0.0)
                & (dff["total_fullGamma"].abs() > 1e-6),
                "total_gammaBreakEven",
            ] = np.sqrt(
                -2
                * valid_befg_df["total_theta"]
                / (valid_befg_df["multiplier"] * valid_befg_df["total_fullGamma"])
            )

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
