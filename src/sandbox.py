from data_connections import conn
import pandas as pd
from sql_utils import strike_range

import json, colorlover

# data = conn.get("greekpositions_xext:dev")

# if data != None:
#     dff = pd.read_json(data)

# print(dff.columns)


def strikeRisk(portfolio, riskType, relAbs, zeros=False):
    # pull list of porducts from static data
    data = conn.get("greekpositions_xext:dev")
    if data != None:
        portfolioGreeks = pd.read_json(data)

        products = portfolioGreeks[
            (portfolioGreeks.portfolio == portfolio) & (portfolioGreeks.strike)
        ]["contract_symbol"].unique()

        # setup greeks and products bucket to collect data
        greeks = []
        dfData = []

        # if zeros build strikes from product
        if zeros:
            allStrikes = strikes(product)

        if relAbs == "strike":
            # for each product collect greek per strike
            for product in products:
                data = portfolioGreeks[portfolioGreeks.contract_symbol == product]
                strikegreeks = []

                if zeros:
                    strikes = allStrikes
                else:
                    strikes = data["strike"]
                # go over strikes and uppack greeks

                strikeRisk = {}
                for strike in strikes:
                    # pull product mult to convert greeks later
                    if strike in data["strike"].astype(int).tolist():
                        risk = data.loc[data.strike == strike][riskType].sum()
                    else:
                        risk = 0

                    strikegreeks.append(risk)
                    strikeRisk[round(strike)] = risk
                greeks.append(strikegreeks)
                dfData.append(strikeRisk)

            df = pd.DataFrame(dfData, index=products)

            # if zeros then reverse order so both in same order
            if not zeros:
                df = df.iloc[:, ::-1]
            df.fillna(0, inplace=True)

            return df.round(3), products
    else:
        return None, None


def discrete_background_color_bins(df, n_bins=4, columns="all"):
    bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]

    if columns == "all":
        if "id" in df:
            df_numeric_columns = df.select_dtypes("number").drop(["id"], axis=1)
        else:
            df_numeric_columns = df.select_dtypes("number")
    else:
        df_numeric_columns = df[columns]

    df_max = df_numeric_columns.max().max()
    df_min = df_numeric_columns.min().min()

    styles = []

    # build ranges
    ranges = [(df_max * i) for i in bounds]

    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(bounds))
        backgroundColor = colorlover.scales[half_bins]["seq"]["Greens"][i - 1]
        color = "black"
        for column in df_numeric_columns:
            styles.append(
                {
                    "if": {
                        "filter_query": (
                            "{{{column}}} >= {min_bound}"
                            + (
                                " && {{{column}}} < {max_bound}"
                                if (i < len(ranges) - 1)
                                else ""
                            )
                        ).format(
                            column=column, min_bound=min_bound, max_bound=max_bound
                        ),
                        "column_id": str(column),
                    },
                    "backgroundColor": backgroundColor,
                    "color": color,
                }
            )

    # build ranges
    ranges = [(df_min * i) for i in bounds]
    for i in range(1, len(ranges)):
        min_bound = ranges[i - 1]
        max_bound = ranges[i]
        half_bins = str(len(ranges))
        backgroundColor = colorlover.scales[half_bins]["seq"]["Reds"][i - 1]
        color = "black"
        for column in df_numeric_columns:
            styles.append(
                {
                    "if": {
                        "filter_query": (
                            "{{{column}}} <= {min_bound}"
                            + (
                                " && {{{column}}} > {max_bound}"
                                if (i < len(ranges) - 1)
                                else ""
                            )
                        ).format(
                            column=column, min_bound=min_bound, max_bound=max_bound
                        ),
                        "column_id": str(column),
                    },
                    "backgroundColor": backgroundColor,
                    "color": color,
                }
            )

    # add zero color
    for column in df_numeric_columns:
        styles.append(
            {
                "if": {
                    "filter_query": ("{{{column}}} = 0").format(column=column),
                    "column_id": str(column),
                },
                "backgroundColor": "rgb(255,255,255)",
                "color": color,
            }
        )
    return styles


def update_greeks(portfolio, riskType, relAbs, zeros):
    # pull dataframe and products
    df, products = strikeRisk(portfolio, riskType, relAbs, zeros=zeros)

    if df.empty:
        return [{}], [], no_update
    else:
        # create columns
        columns = [{"id": "product", "name": "Product"}] + [
            {"id": str(i), "name": str(i)} for i in sorted(df.columns.values)
        ]

        df["product"] = products
        # create data
        df = df.loc[~(df["product"] == "None")]

        # convert column names to strings fo json
        df.columns = df.columns.astype(str)

        # sort based on product name
        df[["first_value", "last_value"]] = df["product"].str.extract(r"([ab])?(\d)")
        df = df.sort_values(by=["first_value", "last_value"])
        df.drop(columns=["last_value", "first_value"], inplace=True)

        data = df.to_dict("records")

        styles = discrete_background_color_bins(df)

        return data, columns, styles


print(strike_range("xext-ebm-eur"))
