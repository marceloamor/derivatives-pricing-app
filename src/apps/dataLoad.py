import base64
import datetime as dt
import io
import traceback

import dash_bootstrap_components as dbc
import orjson
import pandas as pd
import sftp_utils
import sqlalchemy.orm
from dash import callback_context, dcc, html
from dash import dash_table as dtable
from dash.dependencies import Input, Output, State
from data_connections import PostGresEngine, conn, shared_engine
from parts import (
    dev_key_redis_append,
    rec_sol3_rjo_cme_pos,
    recRJO,
    rjo_to_sol3_hash,
    sendEURVolsToPostgres,
    settleVolsProcess,
    topMenu,
)

# options for file type dropdown
fileOptions = [
    {"label": "LME Vols", "value": "lme_vols"},
    {"label": "EUR Vols", "value": "eur_vols"},
    {"label": "ICE Vols", "value": "ice_vols"},
    {"label": "Rec LME Positions", "value": "rec_lme_pos"},
    {"label": "Rec CME Positions", "value": "rec_cme_pos"},
    {"label": "Rec Euronext Positions", "value": "rec_euro_pos"},
]

# layout for dataload page
layout = html.Div(
    [
        topMenu("Data Load"),
        dcc.Dropdown(
            id="file_type", value=fileOptions[0]["value"], options=fileOptions
        ),
        dbc.Button("rec-button", id="rec-button", n_clicks=0),
        dcc.Upload(
            id="upload-data",
            children=html.Div(["Drag and Drop or ", html.A("Select Vols")]),
            style={
                "width": "100%",
                "height": "60px",
                "lineHeight": "60px",
                "borderWidth": "1px",
                "borderStyle": "dashed",
                "borderRadius": "5px",
                "textAlign": "center",
                "margin": "10px",
            },
            # Allow multiple files to be uploaded
            multiple=True,
        ),
        dcc.ConfirmDialog(
            id="confirm-upload",
            message="Vols appear to be in Absolute format, not Relative! Upload anyway?",
        ),
        html.Div(id="output-data-upload"),
        html.Div(id="output-confirm-upload"),
        html.Div(id="sol3-rjo-filenames"),
        dcc.Loading(
            id="loading-2",
            children=[html.Div([html.Div(id="output-rec-button")])],
            type="circle",
        ),
        # html.Div(id="output-rec-button"),
    ]
)


# function to parse data file
def parse_data(contents, filename, input_type=None):
    content_type, content_string = contents.split(",")

    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename:
            # Assume that the user uploaded a CSV or TXT file
            if input_type in ["rec", "rec_trades", "rec_cme_pos"]:
                df = pd.read_csv(
                    io.StringIO(decoded.decode("utf-8")),
                    skiprows=1,
                )
            else:
                # LME Vols
                df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))

        elif "xls" in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
        elif "txt" or "tsv" in filename:
            # Assume that the user upl, delimiter = r'\s+'oaded an excel file
            df = pd.read_csv(io.StringIO(decoded.decode("utf-8")), delimiter=r"\s+")
    except Exception as e:
        print(e)
        return html.Div(["There was an error processing this file."])

    return df


# validate lme vols, handle absolute/relative format possibility
def validate_lme_vols(df):
    # 4 validation steps
    status = (0, "Vols uploaded successfully")
    # 1- check Date column format: ddMmmYY
    try:
        _ = pd.to_datetime(df["Date"], format="%d%b%y")
    except ValueError:
        status = (1, "Date column not formatted correctly")
    # 2- check all dates are the same
    if df["Date"].nunique() > 1:
        status = (1, "Multiple dates in file")
    # 3- check Series column format: MmmYY
    try:
        _ = pd.to_datetime(df["Series"], format="%b%y")
    except ValueError:
        status = (1, "Series column not formatted correctly")
    # 4- check Relative and not Absolute
    diff_columns = ["-10 DIFF", "-25 DIFF", "50 Delta", "+25 DIFF", "+10 DIFF"]
    if (df[diff_columns].max(axis=1) != df["50 Delta"]).any():
        # do something
        status = (2, "Vols appear to be in Absolute format, not Relative")

    return status


def initialise_callbacks(app):
    @app.callback(
        Output("output-data-upload", "children"),
        Output("confirm-upload", "displayed"),
        [Input("upload-data", "contents"), Input("upload-data", "filename")],
        State("file_type", "value"),
    )
    def update_table(contents, filename, file_type):
        # base table holder
        table = html.Div()

        # if contents then translate .csv into table contents.
        if contents:
            if file_type == "lme_vols":
                # un pack and parse data
                contents = contents[0]
                filename = filename[0]
                df = parse_data(contents, filename, "lme_vols")
                # ensure "NA" is read as string and not set to NaN
                df["Product"] = df["Product"].fillna("NA")

                date = df["Date"].iloc[0]

                # validate vols are correct
                status = validate_lme_vols(df)

                # handle relative vs absolute confirm dialog
                if status[0] == 2:
                    return status[1], True

                # load LME vols
                if status[0] == 0:
                    try:
                        table = pd.read_sql(
                            'SELECT DISTINCT "Date" FROM "settlementVolasLME"',
                            con=PostGresEngine(),
                        )

                        if date in table["Date"].values:
                            PostGresEngine().execute(
                                'DELETE FROM "settlementVolasLME" WHERE "Date" = %s',
                                (date,),
                            )

                        # add current vols to end of settlement volas in SQL DB
                        df.to_sql(
                            "settlementVolasLME",
                            con=PostGresEngine(),
                            if_exists="append",
                            index=False,
                        )

                        # reprocess vols in prep
                        settleVolsProcess()
                        return "Sucessfully uploaded Settlement Vols", False

                    except Exception:
                        traceback.print_exc()
                        return f"Failed to load Settlement Vols: {status[1]}", False
                else:
                    return f"Failed to load Settlement Vols: {status[1]}", False

            # TODO: generalise this before reworking
            elif file_type == "ice_vols":
                # this remains terrible but it's a quick fix to get things out,
                # I LOVE BANDAIDS
                monthCode = {
                    "k4": "24-04-12",
                    "m4": "24-06-12",
                    "u4": "24-08-09",
                    "z4": "24-11-08",
                    "h5": "25-02-12",
                    "k5": "25-04-11",
                    "m5": "25-06-12",
                    "u5": "25-08-08",
                    "z5": "25-11-12",
                    "h6": "26-02-11",
                    "k6": "26-04-10",
                    "m6": "26-06-12",
                    "u6": "26-08-14",
                    "z6": "26-11-12",
                }

                def build_symbol(row):
                    prefix = "xice-kc-usd o "
                    instrument = prefix + monthCode[row["code"]] + " a"
                    return instrument

                # un pack and parse data
                contents = contents[0]
                filename = filename[0]
                df = parse_data(contents, filename, "lme_vols")

                df.columns = df.columns.str.lower()

                # interpolate strikes within range
                df = df.set_index("strike")
                new_index = pd.Index(
                    range((int(df.index[0])), (int(df.index[-1] + 1)), 1)
                )
                df = df.reindex(new_index)

                df = df.interpolate(method="polynomial", order=2)
                df = df.reset_index().rename(columns={"index": "strike"})

                # melt dataframe on expiry and build product name
                df = pd.melt(
                    df, id_vars=["date", "strike"], var_name="code", value_name="vol"
                )
                df["option"] = df.apply(build_symbol, axis=1)
                year = "202" + df["code"].iloc[0][1]

                df.rename(
                    columns={
                        "date": "settlement_date",
                        "option": "option_symbol",
                        "vol": "volatility",
                    },
                    inplace=True,
                )

                df = df[["settlement_date", "option_symbol", "strike", "volatility"]]

                date = df["settlement_date"].iloc[0] + "-" + year
                date = dt.datetime.strptime(date, "%d-%b-%Y")
                df["settlement_date"] = date

                try:
                    sendEURVolsToPostgres(df, date)
                    conn.publish(
                        "v2:compute" + dev_key_redis_append,
                        orjson.dumps(
                            {"type": "staticdata", "product_symbol": ["xice-kc-usd"]}
                        ),
                    )
                    return "Sucessfully loaded Euronext Settlement Vols", False
                except Exception as e:
                    print(e)
                    return "There was an error processing this file.", False
            elif file_type == "eur_vols":
                monthCode = {
                    "u3": "23-08-15",
                    "z3": "23-11-15",
                    "h4": "24-02-15",
                    "k4": "24-04-15",
                    "u4": "24-08-15",
                    "z4": "24-11-15",
                    "h5": "25-02-17",
                    "k5": "25-04-15",
                    "u5": "25-08-15",
                    "z5": "25-11-17",
                    "h6": "26-02-16",
                    "k6": "26-04-15",
                    "u6": "26-08-17",
                }

                def build_symbol(row):
                    prefix = "xext-ebm-eur o "
                    instrument = prefix + monthCode[row["code"]] + " a"
                    return instrument

                # un pack and parse data
                contents = contents[0]
                filename = filename[0]
                df = parse_data(contents, filename, "lme_vols")

                df.columns = df.columns.str.lower()

                # interpolate strikes within range
                df = df.set_index("strike")
                new_index = pd.Index(
                    range((int(df.index[0])), (int(df.index[-1] + 1)), 1)
                )
                df = df.reindex(new_index)

                df = df.interpolate(method="polynomial", order=2)
                df = df.reset_index().rename(columns={"index": "strike"})

                # melt dataframe on expiry and build product name
                df = pd.melt(
                    df, id_vars=["date", "strike"], var_name="code", value_name="vol"
                )
                df["option"] = df.apply(build_symbol, axis=1)
                year = "202" + df["code"].iloc[0][1]

                df.rename(
                    columns={
                        "date": "settlement_date",
                        "option": "option_symbol",
                        "vol": "volatility",
                    },
                    inplace=True,
                )

                df = df[["settlement_date", "option_symbol", "strike", "volatility"]]

                date = df["settlement_date"].iloc[0] + "-" + year
                date = dt.datetime.strptime(date, "%d-%b-%Y")
                df["settlement_date"] = date

                try:
                    sendEURVolsToPostgres(df, date)
                    conn.publish(
                        "v2:compute" + dev_key_redis_append,
                        orjson.dumps(
                            {"type": "staticdata", "product_symbol": ["xext-ebm-eur"]}
                        ),
                    )
                    return "Sucessfully loaded Euronext Settlement Vols", False
                except Exception as e:
                    print(e)
                    return "There was an error processing this file.", False

        return table, False

    @app.callback(
        Output("output-confirm-upload", "children"),
        [
            Input("confirm-upload", "submit_n_clicks"),
            Input("confirm-upload", "cancel_n_clicks"),
        ],
    )
    def update_table(y, n):
        ctx = callback_context

        if not ctx.triggered:
            return ""

        if ctx.triggered[0]["prop_id"] == "confirm-upload.submit_n_clicks":
            # user clicks upload anyway
            if y:
                return "Upload continued and processed."

        elif ctx.triggered[0]["prop_id"] == "confirm-upload.cancel_n_clicks":
            # user clicks cancel upload
            if n:
                return "Upload was canceled."

        return ""

    # LME, CME, or Euronext rec on button click
    @app.callback(
        [
            Output("output-rec-button", "children"),
            Output("sol3-rjo-filenames", "children"),
        ],
        [Input("rec-button", "n_clicks")],
        State("file_type", "value"),
    )
    def rec_button(n, file_type):
        # on click do this
        filenames = html.Div()
        table = html.Div()

        if n > 0:
            if file_type == "rec_cme_pos":
                # get latest sol3 and rjo pos exports
                (
                    latest_sol3_df,
                    latest_sol3_filename,
                ) = sftp_utils.fetch_latest_sol3_export(
                    "positions", "export_positions_cme_%Y%m%d-%H%M.csv"
                )

                (
                    latest_rjo_df,
                    latest_rjo_filename,
                ) = sftp_utils.fetch_latest_rjo_export(
                    "UPETRADING_csvnpos_npos_%Y%m%d.csv"
                )
                # drop all contracts not in sol3 (LME)
                latest_rjo_df = latest_rjo_df[
                    ~latest_rjo_df["Bloomberg Exch Code"].isin(["LME", "EOP"])
                ]
                latest_rjo_df = latest_rjo_df[
                    latest_rjo_df["Contract Code"].isin(list(rjo_to_sol3_hash.keys()))
                ]

                rec = rec_sol3_rjo_cme_pos(latest_sol3_df, latest_rjo_df)
                rec_table = dtable.DataTable(
                    data=rec.to_dict("records"),
                    columns=[
                        {"name": col_name, "id": col_name} for col_name in rec.columns
                    ],
                )
                filename_string = (
                    "Sol3 filename: "
                    + latest_sol3_filename
                    + " RJO filename: "
                    + latest_rjo_filename
                )
                return [rec_table], [filename_string]

            elif file_type == "rec_lme_pos":
                # column titles for output table.
                columns = [
                    {"id": "instrument_symbol", "name": "Instrument"},
                    {"id": "accountnumber", "name": "Account"},
                    {"id": "net_quantity_UPE", "name": "Georgia"},
                    {"id": "net_quantity_RJO", "name": "RJO"},
                    {"id": "diff", "name": "Diff"},
                ]

                # rec current dataframe
                # with shared_engine.connect() as connection:
                #     print(connection.execute(sqlalchemy.text("SELECT 1")))
                with sqlalchemy.orm.Session(shared_engine) as session:
                    rec, latest_rjo_filename = recRJO("LME", session)
                rec.reset_index(inplace=True)
                table = dtable.DataTable(
                    id="recTable",
                    data=rec.to_dict("records"),
                    columns=columns,
                )
                return [table], [latest_rjo_filename]

            elif file_type == "rec_euro_pos":
                # column titles for output table.
                columns = [
                    {"id": "instrument_symbol", "name": "Instrument"},
                    {"id": "accountnumber", "name": "Account"},
                    {"id": "net_quantity_UPE", "name": "Georgia"},
                    {"id": "net_quantity_RJO", "name": "RJO"},
                    {"id": "diff", "name": "Diff"},
                ]

                # rec current dataframe
                with sqlalchemy.orm.Session(shared_engine) as session:
                    rec, latest_rjo_filename = recRJO("EURONEXT", session)
                rec.reset_index(inplace=True)
                table = dtable.DataTable(
                    id="recTable",
                    data=rec.to_dict("records"),
                    columns=columns,
                )
                return [table], [latest_rjo_filename]

        return table, filenames
