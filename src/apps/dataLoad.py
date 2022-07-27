import io, base64
from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
from dash import dash_table as dtable
import pandas as pd
import dash_bootstrap_components as dbc

from parts import settleVolsProcess
from data_connections import PostGresEngine, conn

from parts import topMenu, recBGM

# options for file type dropdown
fileOptions = [
    {"label": "LME Vols", "value": "lme_vols"},
    {"label": "Rec Positions", "value": "rec"},
]

# layout for dataload page
layout = html.Div(
    [
        topMenu("Data Load"),
        dcc.Dropdown(
            id="file_type", value=fileOptions[0]["value"], options=fileOptions
        ),
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
    ]
)

# function to parse data file
def parse_data(contents, filename, input_type=None):
    content_type, content_string = contents.split(",")

    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename:
            # Assume that the user uploaded a CSV or TXT file
            if input_type == "rec":
                df = pd.read_csv(io.StringIO(decoded.decode("utf-8")), skiprows=1)
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

                except:

                    return "Failed to load Settlement Vols"

            if file_type == "rec":

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

        return table
