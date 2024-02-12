from parts import topMenu, multiply_rjo_positions
from data_connections import engine, conn

from apps.cashManager import expiry_from_symbol
import sftp_utils

import upestatic

from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from dash import dash_table as dtable
from dash import dcc, html
import dash_daq as daq
import pandas as pd
import numpy as np

import datetime as dt
import json

monthCode = {
    1: "f",
    2: "g",
    3: "h",
    4: "j",
    5: "k",
    6: "m",
    7: "n",
    8: "q",
    9: "u",
    10: "v",
    11: "x",
    12: "z",
}


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

discount_columns = [
    {"name": "Exp. Date", "id": "expiry"},
    {"name": "Market Value", "id": "market_value"},
    {"name": "Interest Due", "id": "interest"},
    {"name": "Interest / Day", "id": "adj_interest_perday"},
]

discount_table = dtable.DataTable(
    id="discount-table",
    columns=discount_columns,
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

liveSwitch = daq.BooleanSwitch(id="static_live", on=False)
liveLabel = html.Label(
    ["Static / Live"], style={"font-weight": "bold", "text-align": "center"}
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
        dbc.Col(html.Div(children=[liveLabel, liveSwitch]), width=1),
        dbc.Col(html.Div(children=""), width=3),
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
        dcc.Store(id="live-store"),
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
                "closeprice",
                "multiplicationfactor",
                "quantity",
                "buysellcode",
            ]
        ]
        # latest_rjo_df.to_csv("rjodf.csv")

        # convert to datetime
        latest_rjo_df["optionexpiredate"] = pd.to_datetime(
            latest_rjo_df["optionexpiredate"], format="%Y%m%d"
        ).dt.date

        # aggregate by expiry, summing market value
        latest_rjo_df = latest_rjo_df.groupby(["optionexpiredate"]).sum(
            numeric_only=True
        )
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
        [
            Input("m2m-rec-store", "data"),
            Input("shockSlider", "value"),
            Input("live-store", "data"),
            Input("static_live", "on"),
        ],
    )
    def display_m2m(data, value, live_data, on):
        if not on:
            if data:
                if value == -5:
                    data = pd.DataFrame(data)
                    data["market_value"] = data["mv_down5"]
                    data["market_value_cum"] = data["market_value_cum_down5"]
                    data = data.drop(
                        columns=["mv_down5", "mv_up5", "market_value_cum_up5"]
                    )
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
        else:
            if live_data:
                data = pd.DataFrame(live_data)
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
            # remove first row
            data = data.iloc[1:]

            cum_mv_left = data["market_value_cum"].iloc[0]

            # deal w positive cum mv
            if cum_mv_left > 0:
                new_data = new_data.append(
                    {
                        "expiry": "No",
                        "market_value": "Discounting",
                        "interest": "Neeeded",
                        "adj_interest_perday": "Today",
                    },
                    ignore_index=True,
                )
                style = [
                    {
                        "if": {"filter_query": '{expiry} = "No"'},
                        # "backgroundColor": "#9960bb",
                        "backgroundColor": "rgb(137, 186, 240)",
                    }
                ]
                return new_data.round(0).to_dict("records"), style

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

            new_data["interest"] = (
                new_data["t"] * abs(new_data["market_value"]) * (rate / 100)
            )
            new_data["interest_perday"] = new_data["interest"] / (new_data["t"] * 365)

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

            # format w commas
            new_data["market_value"] = new_data["market_value"].apply(
                format_with_commas
            )
            new_data["interest"] = new_data["interest"].apply(format_with_commas)
            new_data["adj_interest_perday"] = new_data["adj_interest_perday"].apply(
                format_with_commas
            )

            if cum_mv_left <= 0:
                # edge case where not enough + market value to offset - market value
                new_data = new_data.append(
                    {
                        "expiry": "Remaining MV:",
                        "market_value": cum_mv_left,
                        "interest": "",
                        "adj_interest_perday": "",
                    },
                    ignore_index=True,
                )

            style = [
                {
                    "if": {"filter_query": '{expiry} = "Total"'},
                    # "backgroundColor": "#9960bb",
                    "backgroundColor": "rgb(137, 186, 240)",
                }
            ]
            # handle non lme exchanges for now
        return new_data.round(0).to_dict("records"), style

    @app.callback(
        Output("live-store", "data"),
        # Output("m2m-rec-table", "data"),
        # Output("m2m-rec-table", "style_data_conditional"),
        # [Input("m2m-rec-store", "data"), Input("static_live", "on")],
        [Input("exchanges", "value")],
    )
    def live_m2m(data):
        # alright what do I need:
        # pull list of all our trades
        # compile them down into a dataframe of expiries, quantities, avg price paid
        #
        # get 3m date, month, and associated key
        three_m = str(json.loads(conn.get("3m")))
        third_m_date = dt.datetime.strptime(three_m, "%Y%m%d")
        third_month_code = monthCode[third_m_date.month] + str(third_m_date.year)[-1]

        # SELECT * FROM public.lme_positions_from_trades
        sql = "SELECT * FROM public.lme_positions_from_trades"
        # df = pd.read_sql(sql, engine)
        df = pd.read_csv("pos_from_trades_test.csv")

        # add a metals column
        df["product"] = df["instrument_symbol"].str.slice(start=0, stop=3)

        # add expiry column
        df["expiry"] = df["instrument_symbol"].apply(expiry_from_symbol)

        # filter for past expiry
        df = df[df["expiry"] > dt.date.today()]

        # add a cop column
        df["cop"] = df["instrument_symbol"].apply(
            lambda x: "C" if x[-1] == "C" else ("P" if x[-1] == "P" else "F")
        )
        # Apply the function to create the 'strike' column
        df["strike"] = df.apply(get_strike_from_symbol, axis=1)

        df["price"] = -1

        # # split based on product
        grouped_by_product = df.groupby("product")
        dfs = []

        # Process each group
        for product, group_df in grouped_by_product:
            # pull fcp curve
            fcp = json.loads(conn.get(f"lme:xlme-{product.lower()}-usd:fcp:dev"))

            # pull option key for 3m
            params = json.loads(conn.get(f"{product.lower()}o{third_month_code}"))
            params = pd.DataFrame.from_dict(params, orient="index")
            underlying_price = params.iloc[0]["und_calc_price"]

            third_wed = (
                dt.datetime.fromtimestamp(params.iloc[0]["third_wed"] / 1e9)
                .date()
                .strftime("%Y%m%d")
            )

            fcp_third_wed = fcp[third_wed]

            spread = underlying_price - fcp_third_wed

            # GET THE SPREAD!!!!!

            # Split by cop
            cop_groups = group_df.groupby("cop")

            # Process each cop group
            for cop, cop_group_df in cop_groups:
                if cop == "F":
                    # Get price for 'F' cop
                    for index, row in cop_group_df.iterrows():
                        try:
                            fcp_price = fcp[str(row["expiry"]).replace("-", "")]
                            cop_group_df.at[index, "price"] = fcp_price + spread
                        except:
                            cop_group_df.at[index, "price"] = 0
                            print("didnt find it!")

                    #     cop_group_df["price"] = spread
                    # cop_group_df["price"] = cop_group_df.apply(
                    #     lambda row: fcp.get(row["expiry"], 0) + spread, axis=1
                    # )
                    # # cop_group_df["price"] = cop_group_df["expiry"].apply(
                    # #     lambda x: fcp.get(x, 0) + spread
                    # # )
                    dfs.append(cop_group_df)
                else:
                    # split by expiry for other cop values
                    expiry_groups = cop_group_df.groupby("expiry")

                    # list to store individual expiry DataFrames
                    expiry_dfs = []

                    # process each expiry group
                    for expiry, expiry_group_df in expiry_groups:
                        option = (
                            expiry_group_df.iloc[0]["instrument_symbol"]
                            .lower()
                            .split(" ")[0]
                        )
                        opt_params = json.loads(conn.get(option))

                        # group by strike
                        strike_groups = expiry_group_df.groupby("strike")

                        # list to store individual strike DataFrames
                        strike_dfs = []

                        # process each strike group
                        for strike, strike_group_df in strike_groups:
                            index_to_find = f"{option} {strike} {cop.lower()}"
                            param_row = dict(opt_params[index_to_find])
                            price = param_row["calc_price"]
                            strike_group_df["price"] = price
                            strike_dfs.append(strike_group_df)

                        # concatenate strike groups back together
                        expiry_group_df = pd.concat(strike_dfs)
                        expiry_dfs.append(expiry_group_df)

                    # concatenate expiry groups back together
                    cop_group_df = pd.concat(expiry_dfs)

                    # append cop group to the list of DataFrames
                    dfs.append(cop_group_df)

        # concatenate all groups back together
        result_df = pd.concat(dfs)

        # mv for options = vol * mult * price
        # mv for futures = vol * mult * (tradedP - price)
        result_df["mult"] = result_df.apply(
            lambda row: 6 if row["product"] == "LND" else 25, axis=1
        )

        condition_cop_cp = (result_df["cop"] == "C") | (result_df["cop"] == "P")

        # Calculate market_value based on the condition
        result_df["market_value"] = np.where(
            condition_cop_cp,
            result_df["net_quantity"] * result_df["mult"] * result_df["price"],
            (result_df["price"] - result_df["vol_weighted_avg_price"])
            * result_df["net_quantity"]
            * result_df["mult"],
        )

        print(result_df.sort_values(by=["expiry"]).to_string())
        result_df.to_csv("georgia_df.csv")

        result_df = result_df[
            [
                "expiry",
                "market_value",
            ]
        ]

        # aggregate by expiry, summing market value
        df = result_df.groupby(["expiry"]).sum(numeric_only=True)
        df = df.reset_index()

        # # add cumulative market value
        df["market_value_cum"] = df["market_value"][::-1].cumsum()[::-1]

        # styles = discrete_background_color_bins(df)

        return df.round(0).to_dict("records")


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


def get_strike_from_symbol(row):
    if row["cop"] == "F":
        return 0
    else:
        # Split the instrument_symbol and get the second word
        return row["instrument_symbol"].split()[1]
