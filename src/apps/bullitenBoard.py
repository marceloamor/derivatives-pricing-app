from parts import buildOptionsBoard, loadStaticData
from app import app

from dash.dependencies import Input, Output
from dash import dash_table as dtable
from dash import dcc, html
import redis

import os, json

interval = str(1000)

# connect to redis (default to localhost).
redisLocation = os.getenv("REDIS_LOCATION", default="localhost")
conn = redis.Redis(redisLocation)


def onLoadProduct():
    staticData = loadStaticData()
    # staticData = pd.read_json(staticData)
    staticData.sort_values("product")
    products = []
    for product in staticData["product"]:
        products.append(product)
    return products, products[0]


productDropdown = html.Div(
    [
        dcc.Dropdown(
            id="boardProduct",
            value=onLoadProduct()[1],
            options=[{"label": name, "value": name} for name in onLoadProduct()[0]],
        ),
        # options =  onLoadProduct()[0])
    ],
    className="two columns",
)

optionsBoard = html.Div(
    [
        dtable.DataTable(
            id="optionsBoard",
            data=[{}],
            # sortColumn='Strike',  # set sorting by strikes
            # sortDirection='ASC',
            # style_cell_conditional=[
            #                {
            #                    'if': {'row_index': 'odd'},
            #                    'backgroundColor': 'rgb(248, 248, 248)'
            #                }]
        )
    ],
    className="eight columns",
)

layout = html.Div(
    [
        dcc.Interval(id="boardlive-update", interval=interval),
        html.H3("Options Board"),
        html.Div([dcc.Link("Home", href="/")], className="row"),
        html.Div(
            [
                "All prices are ilistration only to trade please call Mark Christou on XXXXXXXXXXX"
            ],
            className="row",
        ),
        html.Div(
            [
                productDropdown,
                html.Div(
                    [
                        dcc.RadioItems(
                            "volPrice",
                            options=[
                                {"label": "Volatility", "value": "vol"},
                                {"label": "Premium", "value": "prem"},
                            ],
                        )
                    ],
                    className="two columns",
                ),
            ]
        ),
        optionsBoard,
    ]
)


@app.callback(
    Output("optionsBoard", "rows"),
    [
        Input("boardlive-update", "interval"),
        Input("boardProduct", "value"),
        Input("volPrice", "value"),
    ],
)
def load_table(interval, product, volPrice):
    data = conn.get(product.lower() + "Board")

    if data:
        data = json.loads(data)
        dff = buildOptionsBoard(data, volPrice)
        dff.sort_index(inplace=True)
        return dff.to_dict("records")

    else:
        return "No Data"
