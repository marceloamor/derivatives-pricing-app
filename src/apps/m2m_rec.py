from parts import topMenu, multiply_rjo_positions
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

shockSlider = dcc.Slider(-5, 5, 5, value=0, id="shockSlider")
shockLabel = html.Label(
    ["% Shock:"], style={"font-weight": "bold", "text-align": "center"}
)

options = dbc.Row(
    [
        dbc.Col(html.Div(children=[exchangeLabel, exchangeDropdown]), width=3),
        dbc.Col(html.Div(children=[shockLabel, shockSlider]), width=3),
    ]
)


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
        # hidden div to store
        dcc.Loading(
            children=[
                dcc.Store(id="m2m-rec-store"),
            ],
            type="circle",
        ),
    ]
)


def initialise_callbacks(app):
    @app.callback(
        Output("m2m-rec-store", "data"),
        [Input("exchanges", "value")],
    )
    def pull_m2m_data(exchange):
        # handle non lme exchanges for now
        if exchange != "lme":
            df = {
                "expiry": ["This functionality"],
                "market_value": ["will be "],
                "market_value_cum": ["coming soon"],
            }

            return pd.DataFrame(df).to_dict("records"), [{}]

        # pull data from sftp
        (latest_rjo_df, latest_rjo_filename) = sftp_utils.fetch_latest_rjo_export(
            "UPETRADING_csvnpos_npos_%Y%m%d.csv"
        )

        # format column names
        latest_rjo_df.columns = latest_rjo_df.columns.str.replace(" ", "")
        latest_rjo_df.columns = latest_rjo_df.columns.str.lower()

        # filter for positions only
        latest_rjo_df = latest_rjo_df[latest_rjo_df["recordcode"] == "P"]

        # filter for selected exchange
        latest_rjo_df = latest_rjo_df[
            latest_rjo_df["bloombergexchcode"] == exchange.upper()
        ]

        # apply price shocks to underlying
        latest_rjo_df = apply_price_shocks(latest_rjo_df)

        # filter for just the columns we need
        latest_rjo_df = latest_rjo_df[
            [
                "optionexpiredate",
                "marketvalue",
                "mv_down5",
                "mv_up5",
            ]
        ]

        # convert to datetime
        latest_rjo_df["optionexpiredate"] = pd.to_datetime(
            latest_rjo_df["optionexpiredate"], format="%Y%m%d"
        ).dt.date

        # aggregate by expiry, summing market value
        latest_rjo_df = latest_rjo_df.groupby(["optionexpiredate"]).sum()
        latest_rjo_df = latest_rjo_df.reset_index()

        # rename columns
        latest_rjo_df = latest_rjo_df.rename(
            columns={
                "optionexpiredate": "expiry",
                "marketvalue": "market_value",
            }
        )

        # add cumulative market value
        latest_rjo_df["market_value_cum"] = latest_rjo_df["market_value"][
            ::-1
        ].cumsum()[::-1]
        latest_rjo_df["market_value_cum_down5"] = latest_rjo_df["mv_down5"][
            ::-1
        ].cumsum()[::-1]
        latest_rjo_df["market_value_cum_up5"] = latest_rjo_df["mv_up5"][::-1].cumsum()[
            ::-1
        ]

        return latest_rjo_df.round(0).to_dict("records")  # , styles

    @app.callback(
        Output("m2m-rec-table", "data"),
        Output("m2m-rec-table", "style_data_conditional"),
        [Input("m2m-rec-store", "data"), Input("shockSlider", "value")],
    )
    def display_m2m(data, value):
        if data:
            if value == -5:
                data = pd.DataFrame(data)
                data["market_value"] = data["mv_down5"]
                data["market_value_cum"] = data["market_value_cum_down5"]
                data = data.drop(columns=["mv_down5", "mv_up5", "market_value_cum_up5"])
            elif value == 5:
                data = pd.DataFrame(data)
                data["market_value"] = data["mv_up5"]
                data["market_value_cum"] = data["market_value_cum_up5"]
                data = data.drop(
                    columns=["mv_down5", "mv_up5", "market_value_cum_down5"]
                )
            elif value == 0:
                data = pd.DataFrame(data)
                data = data.drop(
                    columns=[
                        "mv_down5",
                        "mv_up5",
                        "market_value_cum_down5",
                        "market_value_cum_up5",
                    ]
                )

            styles = discrete_background_color_bins(data)
            # handle non lme exchanges for now
            return data.round(0).to_dict("records"), styles


# colour bins for market value column, and highlight min cumulative value
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
    cum_sum_min = df["market_value_cum"].min() + 1

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
        # THIS LAST ONE IS TO ADD HIGHLIGHTING IN THE CUMULATIVE COLUMN
        {
            "if": {
                "column_id": "market_value_cum",
                "filter_query": "{{market_value_cum}} <= {}".format(cum_sum_min),
            },
            "backgroundColor": red_2,
            "color": "black",
        },
    ]
    return styles


# apply +-5% shocks to underlying, options unaffected
def apply_price_shocks(df):
    # get vol from quanitity and buy/sell code
    df["vol"] = df.apply(multiply_rjo_positions, axis=1)

    # get 5% shock value
    df["5pc"] = df["closeprice"] * 0.05

    # set default values that options will use
    df["mv_down5"] = df["marketvalue"]
    df["mv_up5"] = df["marketvalue"]

    # generate mask to filter for options, and apply shocks to futures
    mask = df["securitysubtypecode"].isin(["C", "P"])

    df.loc[~mask, "mv_down5"] = (
        (df["closeprice"] - df["5pc"] - df["formattedtradeprice"])
        * df["vol"]
        * df["multiplicationfactor"]
    )

    df.loc[~mask, "mv_up5"] = (
        (df["closeprice"] + df["5pc"] - df["formattedtradeprice"])
        * df["vol"]
        * df["multiplicationfactor"]
    )
    return df
