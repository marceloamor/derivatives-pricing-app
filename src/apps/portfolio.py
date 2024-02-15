import os

import dash_bootstrap_components as dbc
import numpy as np
import orjson
import pandas as pd
from dash import dash_table as dtable
from dash import dcc, html
from dash.dependencies import Input, Output
from data_connections import Session, conn
from parts import ringTime, topMenu
from upedata import static_data as upe_static

if os.getenv("USE_DEV_KEYS") == "True":
    pass


positionLocationLME = os.getenv("POS_LOCAITON_LME", default="greekpositions")
positionLocationEUR = os.getenv("POS_LOCAITON_EUR", default="greekpositions_xext:dev")


def get_portfolio_info():
    """
    pulls portfolio info from static data in the two formats required for portfolio page
    1. dash dict of display_name: portfolio_id to keep portfolio dropdown dynamic
    2. dict of portfolio_id: display_name to to map portfolio_id to display_name in the table
    """
    with Session() as session:
        query = session.query(upe_static.Portfolio).all()
        port_dict = {}
        port_options = [{"label": "All", "value": "all"}]
        for port in query:
            port_dict[port.portfolio_id] = port.display_name
            if port.display_name != "Error":
                port_options.append(
                    {"label": port.display_name, "value": port.portfolio_id}
                )
    return (port_dict, port_options)


port_dict, port_options = get_portfolio_info()

# 1 sec interval
interval = str(1000 * 1)

# columns = [
#     {"name": "Product", "id": "contract_symbol"},
#     {"name": "Portfolio", "id": "portfolio"},
#     {"name": "Delta", "id": "total_delta"},
#     {"name": "Full Delta", "id": "total_fullDelta"},
#     {"name": "Vega", "id": "total_vega"},
#     {"name": "Theta", "id": "total_theta"},
#     {"name": "Gamma", "id": "total_gamma"},
#     {"name": "Full Gamma", "id": "total_fullGamma"},
#     {"name": "Delta Decay", "id": "total_deltaDecay"},
#     {"name": "Vega Decay", "id": "total_vegaDecay"},
#     {"name": "Gamma Decay", "id": "total_gammaDecay"},
#     {"name": "Gamma Breakeven", "id": "total_gammaBreakEven"},
# ]
columns = [
    {"name": "Product", "id": "instrument_symbol"},
    {"name": "Portfolio", "id": "portfolio_name"},
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


# product dropdown and label
# product_options = onLoadPortFolio()
# product_options.append({"label": "Milling Wheat", "value": "xext-ebm-eur"})
def loadProducts():
    with Session() as session:
        products = session.query(upe_static.Product).all()
        return products


product_options = [
    {"label": product.long_name.title(), "value": product.symbol}
    for product in loadProducts()
]

productDropdown = dcc.Dropdown(
    id="product-selector",
    value="xlme-lcu-usd",  # autoselect copper
    options=product_options,
)
productLabel = html.Label(
    ["Product:"], style={"font-weight": "bold", "text-align": "left"}
)

# portfolio dropdown and label
portfolioDropdown = dcc.Dropdown(
    id="portfolio-selector",
    value="all",
    options=port_options,
)
portfolioLabel = html.Label(
    ["Portfolio:"], style={"font-weight": "bold", "text-align": "left"}
)


selectors = dbc.Row(
    [
        dbc.Col(
            [productLabel, productDropdown],
            width=3,
        ),
        dbc.Col(
            [portfolioLabel, portfolioDropdown],
            width=3,
        ),
    ]
)


table = dbc.Row(
    [
        dbc.Col(
            [
                dtable.DataTable(
                    id="portfolios",
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

layout = html.Div(
    [
        topMenu("Portfolio Risk"),
        dcc.Interval(
            id="live-update",
            interval=1 * 1000,
            n_intervals=0,  # in milliseconds
        ),
        selectors,
        table,
    ]
)


def initialise_callbacks(app):
    # pull greeks
    @app.callback(
        Output("portfolios", "data"),
        [
            Input("live-update", "n_intervals"),
            Input("product-selector", "value"),
            Input("portfolio-selector", "value"),
        ],
    )
    def update_greeks(interval, product, portfolio):
        # update w new pos_eng output
        if product:
            # get data from pos_eng
            df = conn.get("pos-eng:greek-positions:dev").decode("utf-8")
            # load into pandas df
            df = pd.DataFrame(orjson.loads(df))
            df = df.groupby("instrument_symbol", as_index=False).sum(numeric_only=True)
            # create new column, product, from the first word in the instrument_symbol
            df["product"] = df["instrument_symbol"].str.split(" ").str[0]
            # create new column, portfolio_name, from the portfolio_id and port_dict
            df["portfolio_name"] = df["portfolio_id"].map(port_dict)

            # filter on product
            df = df[df["product"] == product]
            # filter on portfolio
            if portfolio != "all":
                df = df[df["portfolio_id"] == portfolio]

            # calculate gamma breakeven
            df["multiplier"] = 50
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

            # # sort on expiry --- column called 't_to_expirt' from pos_eng
            df.sort_values("t_to_expiry", inplace=True)
            # df.sum(numeric_only=True, axis=0)

            # # sort by date
            # df["expiry"] = df["contract_symbol"].str.split(" ").str[2]
            # df.sort_values("expiry", inplace=True, ascending=True)

            # calc total row and re label
            df.loc["Total"] = df.sum(numeric_only=True, axis=0)
            df.loc["Total", "instrument_symbol"] = "Total"

            # still need to finish this i believe
            return df.round(3).to_dict("records")

        if portfolio == "xext-ebm-eur":
            # pull list of porducts from static data
            data = conn.get(positionLocationEUR).decode("utf-8")
            if data != None:
                dff = pd.read_json(data)
                # aggregate by product name
                dff = dff.groupby("contract_symbol", as_index=False).sum(
                    numeric_only=True
                )
                dff["multiplier"] = 50
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

                # sort on expiry
                dff.sort_values("T_cal_to_underlying_expiry", inplace=True)
                dff.sum(numeric_only=True, axis=0)

                # sort by date
                dff["expiry"] = dff["contract_symbol"].str.split(" ").str[2]
                dff.sort_values("expiry", inplace=True, ascending=True)

                # calc total row and re label
                dff.loc["Total"] = dff.sum(numeric_only=True, axis=0)
                dff.loc["Total", "contract_symbol"] = "Total"

                return dff.round(3).to_dict("records")
        else:
            dff = conn.get(positionLocationLME).decode("utf-8")
            dff = pd.read_json(dff)

            if not dff.empty:
                # sort on expiry
                dff.sort_values("expiry", inplace=True)
                # group on product
                dff = (
                    dff[dff["portfolio"] == product]
                    .groupby("product")
                    .sum(numeric_only=True)
                    .round(3)
                    .reset_index()
                )
                dff["total_gammaBreakEven"] = 0.0

                # sort based on product name
                dff[["first_value", "last_value"]] = dff["product"].str.extract(
                    r"([ab])?(\d)"
                )
                dff = dff.sort_values(by=["first_value", "last_value"])
                dff.drop(columns=["last_value", "first_value"], inplace=True)

                # calc total row and re label
                dff.loc["Total"] = dff.sum(numeric_only=True, axis=0)
                dff.loc["Total", "product"] = "Total"

                # rename product to match euronext
                dff.rename(columns={"product": "contract_symbol"}, inplace=True)

                return dff.round(3).to_dict("records")

    @app.callback(Output("ring", "children"), [Input("live-update", "n_intervals")])
    def updareRing(interval):
        return ringTime()
