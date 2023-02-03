import io, base64
from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import pandas as pd
import dash_bootstrap_components as dbc

from parts import settleVolsProcess
from data_connections import PostGresEngine, conn

from parts import (
    topMenu,
    recBGM,
    rec_britannia_mir13,
    rec_sol3_cme_pos_bgm_mir_14,
    rec_sol3_rjo_cme_pos,
)

import sftp_utils

import traceback

# options for file type dropdown
fileOptions = [
    {"label": "LME Vols", "value": "lme_vols"},
    {"label": "Rec LME Positions (MIR14)", "value": "rec"},
    {"label": "Rec LME Trades (MIR13)", "value": "rec_trades"},
    {"label": "Rec CME Positions (MIR14)", "value": "rec_cme_pos"},
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
            children=html.Div(["Drag and Drop or ", html.A("Select Files")]),
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
        html.Div(id="output-data-upload"),
        html.Div(id="sol3-rjo-filenames"),
        html.Div(id="output-rec-button"),
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


def initialise_callbacks(app):
    @app.callback(
        Output("output-data-upload", "children"),
        [Input("upload-data", "contents"), Input("upload-data", "filename")],
        State("file_type", "value"),
    )
    def update_table(contents, filename, file_type):

        # base table holder
        table = html.Div()

        # if contents then translate .csv into table contents.
        if contents:

            # un pack and parse data
            contents = contents[0]
            filename = filename[0]
            df = parse_data(contents, filename, file_type)

            # load LME vols
            if file_type == "lme_vols":
                try:

                    # add current vols to end of settlement volas in SQL DB
                    df.to_sql(
                        "settlementVolasLME",
                        con=PostGresEngine(),
                        if_exists="append",
                        index=False,
                    )

                    # reprocess vols in prep
                    settleVolsProcess()
                    return "Sucessfully loads Settlement Vols"

                except Exception as e:
                    traceback.print_exc()
                    return "Failed to load Settlement Vols"

            elif file_type == "rec":
                # column titles for output table.
                columns = [
                    {"id": "instrument", "name": "Instrument"},
                    {"id": "quanitity_UPE", "name": "Georgia"},
                    {"id": "quanitity_BGM", "name": "BGM"},
                    {"id": "diff", "name": "Break"},
                ]

                # rec current dataframe
                rec = recBGM(df)
                rec["instrument"] = rec.index
                table = dbc.Col(
                    dtable.DataTable(
                        id="recTable",
                        data=rec.to_dict("records"),
                        columns=columns,
                    )
                )
                return html.Div(table)

            elif file_type == "rec_trades":
                rec = rec_britannia_mir13(df)
                return dtable.DataTable(
                    rec.to_dict("records"),
                    [{"name": col_name, "id": col_name} for col_name in rec.columns],
                )

            elif file_type == "rec_cme_pos":
                latest_sol3_df = sftp_utils.fetch_latest_sol3_cme_pos_export()
                rec = rec_sol3_cme_pos_bgm_mir_14(latest_sol3_df, df)
                return html.Div(
                    dtable.DataTable(
                        id="rec_table",
                        data=rec.to_dict("records"),
                        columns=[
                            {"name": col_name, "id": col_name}
                            for col_name in rec.columns
                        ],
                    )
                )

        return table

    # sol3 and rjo pos rec on button click
    @app.callback(
        Output("output-rec-button", "children"),
        Output("sol3-rjo-filenames", "children"),
        [Input("rec-button", "n_clicks")],
    )
    def sol3_rjo_rec_button(n):
        # on click do this
        filenames = html.Div()
        table = html.Div()

        if n > 0:
            # get latest sol3 and rjo pos exports
            sol3_pos = sftp_utils.fetch_latest_sol3_cme_pos_export()
            rjo_pos = sftp_utils.fetch_latest_rjo_cme_pos_export()
            latest_sol3_df = sol3_pos[0]
            latest_rjo_df = rjo_pos[0]
            latest_sol3_filename = sol3_pos[1]
            latest_rjo_filename = rjo_pos[1]

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
            return rec_table, filename_string

        return table, filenames
