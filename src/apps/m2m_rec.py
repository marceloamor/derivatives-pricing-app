from parts import topMenu, multiply_rjo_positions
import sftp_utils

from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html
import pandas as pd

import datetime as dt


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

discount_table = dtable.DataTable(
    id="discount-table",
    columns=[
        {"name": "Exp. Date", "id": "expiry"},
        {"name": "Market Value", "id": "market_value"},
        {"name": "Interest Due", "id": "interest"},
        {"name": "Interest / Day", "id": "adj_interest_perday"},
    ],
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

rateInput = dcc.Input(
    id="discount_rate_input", type="number", debounce=True, placeholder=7.5
)
rateLabel = html.Label(
    ["Effective Rate (%)"], style={"font-weight": "bold", "text-align": "left"}
)

options = dbc.Row(
    [
        dbc.Col(html.Div(children=[exchangeLabel, exchangeDropdown]), width=3),
        dbc.Col(html.Div(children=[shockLabel, shockSlider]), width=3),
        dbc.Col(html.Div(children=""), width=4),
        dbc.Col(html.Div(children=[rateLabel, rateInput]), width=2),
    ]
)


layout = html.Div(
    [
        topMenu("M2M Rec"),
        options,
        dbc.Row(
            [
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
                dbc.Col(
                    dcc.Loading(
                        id="loading-7",
                        children=[
                            html.Div(
                                discount_table,
                            )
                        ],
                        type="circle",
                    ),
                    width=3,
                ),
            ]
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

        return latest_rjo_df.round(0).to_dict("records")

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

            # data["market_value"] = data["market_value"].apply(format_with_commas)
            # data["market_value_cum"] = data["market_value_cum"].apply(
            #     format_with_commas
            # )
            # handle non lme exchanges for now
            return data.round(0).to_dict("records"), styles

    @app.callback(
        Output("discount-table", "data"),
        Output("discount-table", "style_data_conditional"),
        [Input("m2m-rec-store", "data"), Input("discount_rate_input", "value")],
        State("discount_rate_input", "placeholder"),
    )
    def discounting_m2m(data, rate, rate_p):
        if not rate:
            rate = rate_p
        if data and rate:
            data = pd.DataFrame(data)
            new_data = pd.DataFrame(
                columns=["expiry", "market_value", "market_value_cum"]
            )
            print(data)
            cum_mv = cum_mv_left = data["market_value_cum"].iloc[0]
            print(cum_mv, cum_mv_left)

            data = data[data["market_value"] < 0]

            for index, row in data.iterrows():
                cum_mv_left += abs(row["market_value"])
                if cum_mv_left >= 0:
                    new_data = new_data.append(
                        {
                            "expiry": row["expiry"],
                            "market_value": row["market_value"] + cum_mv_left,
                            "market_value_cum": row["market_value_cum"],
                        },
                        ignore_index=True,
                    )

                    print(row)
                    break
                new_data = new_data.append(
                    {
                        "expiry": row["expiry"],
                        "market_value": row["market_value"],
                        "market_value_cum": row["market_value_cum"],
                    },
                    ignore_index=True,
                )

            new_data["expiry"] = pd.to_datetime(
                new_data["expiry"], format="%Y-%m-%d"
            ).dt.date
            new_data["t"] = new_data["expiry"].apply(
                lambda x: (x - dt.date.today()).days / 365
            )
            print(dt.date.today())
            new_data["interest"] = (
                new_data["t"] * abs(new_data["market_value"]) * (rate / 100)
            )
            new_data["interest_perday"] = new_data["interest"] / (new_data["t"] * 365)
            # cumulative_sum = new_data["interest_perday"].cumsum()
            # new_data["adj_interest_perday"] = cumulative_sum[::-1].cumsum()[::-1]
            new_data["adj_interest_perday"] = new_data["interest_perday"][
                ::-1
            ].cumsum()[::-1]

            # format and return data
            new_data = new_data.append(
                {
                    "expiry": "Total",
                    "market_value": new_data["market_value"].sum(),
                    "interest": new_data["interest"].sum(),
                    "adj_interest_perday": "",
                },
                ignore_index=True,
            )

            print("int to pay today: ", new_data["interest_perday"].sum())

            # format w commas
            new_data["market_value"] = new_data["market_value"].apply(
                format_with_commas
            )
            new_data["interest"] = new_data["interest"].apply(format_with_commas)
            new_data["adj_interest_perday"] = new_data["adj_interest_perday"].apply(
                format_with_commas
            )

            style = [
                {
                    "if": {"filter_query": '{expiry} = "Total"'},
                    # "backgroundColor": "#9960bb",
                    "backgroundColor": "rgb(137, 186, 240)",
                }
            ]
            print(new_data)

            # handle non lme exchanges for now
        return new_data.round(0).to_dict("records"), style


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


def format_with_commas(x):
    if isinstance(x, (int, float)):
        return "{:,.0f}".format(x)
    else:
        return x
