from parts import topMenu
import sftp_utils

from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html
import pandas as pd


columns = [
    {"name": "Exp. Date", "id": "expiry"},
    {"name": "Market Value", "id": "market_value"},
    {"name": "Cum. Market Value", "id": "market_value_cum"},
]

table = dtable.DataTable(
    id="m2m-rec-table",
    columns=columns,
    data=[{}],
    style_data={"textAlign": "right"},
    style_data_conditional=[{}],
)


exchangeList = [
    {"label": "LME", "value": "lme"},
    {"label": "CME", "value": "cmx"},
    {"label": "Euronext", "value": "eop"},
]

exchangeDropdown = dcc.Dropdown(
    id="exchanges",
    options=exchangeList,
    value=exchangeList[0]["value"],
    clearable=False,
)
exchangeLabel = html.Label(
    ["Exchange:"], style={"font-weight": "bold", "text-align": "left"}
)
options = dbc.Col(html.Div(children=[exchangeLabel, exchangeDropdown]), width=3)


layout = html.Div(
    [
        topMenu("M2M Rec"),
        options,
        dbc.Col(
            dcc.Loading(
                id="loading-5",
                children=[
                    html.Div(
                        table,
                    )
                ],
                type="circle",
            ),
        ),
    ]
)


def initialise_callbacks(app):
    @app.callback(
        Output("m2m-rec-table", "data"),
        Output("m2m-rec-table", "style_data_conditional"),
        [Input("exchanges", "value")],
    )
    def m2m(exchange):
        # handle non lme exchanges for now
        if exchange != "lme":
            df = {
                "expiry": ["This functionality"],
                "market_value": ["will be "],
                "market_value_cum": ["coming soon"],
            }

            return pd.DataFrame(df).to_dict("records"), [{}]

        # pull data
        (latest_rjo_df, latest_rjo_filename) = sftp_utils.fetch_latest_rjo_export(
            "UPETRADING_csvnpos_npos_%Y%m%d.csv"
        )

        # filter for positions only
        latest_rjo_df = latest_rjo_df[latest_rjo_df["Record Code"] == "P"]

        # filter for selected exchange
        latest_rjo_df = latest_rjo_df[
            latest_rjo_df["Bloomberg Exch Code"].str.lower() == exchange
        ]

        # filter for just the columns we need
        latest_rjo_df = latest_rjo_df[
            [
                "Option Expire Date",
                "Market Value",
            ]
        ]

        # convert to datetime
        latest_rjo_df["Option Expire Date"] = pd.to_datetime(
            latest_rjo_df["Option Expire Date"], format="%Y%m%d"
        ).dt.date

        # aggregate by expiry, summing market value
        latest_rjo_df = latest_rjo_df.groupby(["Option Expire Date"]).sum()
        latest_rjo_df = latest_rjo_df.reset_index()

        # rename columns
        latest_rjo_df = latest_rjo_df.rename(
            columns={
                "Option Expire Date": "expiry",
                "Market Value": "market_value",
            }
        )

        # add cumulative market value
        latest_rjo_df["market_value_cum"] = latest_rjo_df["market_value"][
            ::-1
        ].cumsum()[::-1]

        # create conditional style sheet
        styles = discrete_background_color_bins(latest_rjo_df)

        return latest_rjo_df.round(0).to_dict("records"), styles


def discrete_background_color_bins(df):
    # 6 bins, 3 green 3 red
    red_1 = "#ffc7c7"
    red_2 = "#ff7676"
    red_3 = "#ff0000"
    green_1 = "#d5ffd3"
    green_2 = "#8eff87"
    green_3 = "#0fff00"

    # get min and max values
    df_max = df["market_value"].max()
    df_min = df["market_value"].min()

    # split into 6 bins
    bins = [df_min, df_min / 2, 0, df_max / 2, df_max]

    styles = [
        # reds
        {
            "if": {
                "filter_query": "{market_value} >= " + str(bins[4]),
                "column_id": "market_value",
            },
            "backgroundColor": red_3,  # reddest
            "fontWeight": "bold",
        },
        {
            "if": {
                "filter_query": "{market_value} <= "
                + str(bins[4])
                + " && {market_value} >= "
                + str(bins[3]),
                "column_id": "market_value",
            },
            "backgroundColor": red_2,  # second reddest
            "fontWeight": "bold",
        },
        {
            "if": {
                "filter_query": "{market_value} <= "
                + str(bins[3])
                + " && {market_value} >= 0",
                "column_id": "market_value",
            },
            "backgroundColor": red_1,  # third reddest
            "fontWeight": "bold",
        },
        # now greens
        {
            "if": {
                "filter_query": "{market_value} <= " + str(bins[0]),
                "column_id": "market_value",
            },
            "backgroundColor": green_3,  # greenest
            "fontWeight": "bold",
        },
        {
            "if": {
                "filter_query": "{market_value} >= "
                + str(bins[0])
                + " && {market_value} <= "
                + str(bins[1]),
                "column_id": "market_value",
            },
            "backgroundColor": green_2,  # second greenest
            "fontWeight": "bold",
        },
        {
            "if": {
                "filter_query": "{market_value} >= "
                + str(bins[1])
                + " && {market_value} <= 0",
                "column_id": "market_value",
            },
            "backgroundColor": green_1,  # third greenest
            "fontWeight": "bold",
        },
    ]

    return styles
