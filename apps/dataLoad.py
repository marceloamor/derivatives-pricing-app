import io, base64, pickle
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd

from parts import settleVolsProcess
from data_connections import PostGresEngine, conn

fileOptions = [{'label': 'LME Vols' , 'value':'lme_vols'},
               ]  

from app import app, topMenu
layout = html.Div(
    [
        dcc.Dropdown(id = 'file_type', value = fileOptions[0]['value'], options = fileOptions),
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

#function to parse data file
def parse_data(contents, filename):
    content_type, content_string = contents.split(",")

    decoded = base64.b64decode(content_string)
    try:
        if "csv" in filename:
            # Assume that the user uploaded a CSV or TXT file
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

@app.callback(
    Output("output-data-upload", "children"),
    [Input("upload-data", "contents"), Input("upload-data", "filename")],
    State('file_type', 'value')
)
def update_table(contents, filename, file_type):
    table = html.Div()

    if contents:
        contents = contents[0]
        filename = filename[0]
        df = parse_data(contents, filename)
        if file_type =='lme_vols':
            try:
                df.to_sql('settlementVolasLME', con=PostGresEngine(), if_exists='append', index=False) 
                settleVolsProcess()   
                return 'Sucessfully loads Settlement Vols'       
            except:
                return 'Failed to load Settlement Vols'

    return table
