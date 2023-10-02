from dash.dependencies import Input, Output, State
from dash import dcc
from dash import dcc, html
import dash_table as dt
import json
import datetime as dt

from parts import retriveSettings, sumbiSettings, onLoadProductProducts

from parts import topMenu


def settingsUpdateCheck(new, old):
    if new == None or not new:
        if old == None or old == " ":
            return " "
        else:
            old = str(old)
            return float(old)
    else:
        return float(new)


# layouts
productDropdown = html.Div(
    [
        dcc.Dropdown(
            id="product",
            value=onLoadProductProducts()[1],
            options=[
                {"label": name, "value": name} for name in onLoadProductProducts()[0]
            ],
        ),
        # options =  onLoadProduct()[0])
    ],
    className="row",
)

# all the inputs and labels
inputs = html.Div(
    [
        html.Div(
            [
                html.Div("Size", className="two columns"),
                html.Div([dcc.Input(id="size", type="text")], className="two columns"),
                html.Div(id="Csize", className="two columns"),
            ],
            className="row",
        ),
        html.H2("Vol", className="row"),
        html.Div(
            [
                html.Div("Edge Multiplier", className="two columns"),
                html.Div(
                    [dcc.Input(id="volEdge", type="text")], className="two columns"
                ),
                html.Div(id="CvolEdge", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Bid Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="volBidAsym", type="text")], className="two columns"
                ),
                html.Div(id="CvolBidAsym", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Ask Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="volAskAsym", type="text")], className="two columns"
                ),
                html.Div(id="CvolAskAsym", className="two columns"),
            ],
            className="row",
        ),
        html.H2("Skew", className="row"),
        html.Div(
            [
                html.Div("Edge Multiplier", className="two columns"),
                html.Div(
                    [dcc.Input(id="skewEdge", type="text")], className="two columns"
                ),
                html.Div(id="CskewEdge", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Bid Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="skewBidAsym", type="text")], className="two columns"
                ),
                html.Div(id="CskewBidAsym", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Ask Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="skewAskAsym", type="text")], className="two columns"
                ),
                html.Div(id="CskewAskAsym", className="two columns"),
            ],
            className="row",
        ),
        html.H2("Call", className="row"),
        html.Div(
            [
                html.Div("Edge Multiplier", className="two columns"),
                html.Div(
                    [dcc.Input(id="callEdge", type="text")], className="two columns"
                ),
                html.Div(id="CcallEdge", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Bid Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="callBidAsym", type="text")], className="two columns"
                ),
                html.Div(id="CcallBidAsym", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Ask Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="callAskAsym", type="text")], className="two columns"
                ),
                html.Div(id="CcallAskAsym", className="two columns"),
            ],
            className="row",
        ),
        html.H2("Put", className="row"),
        html.Div(
            [
                html.Div("Edge Multiplier", className="two columns"),
                html.Div(
                    [dcc.Input(id="putEdge", type="text")], className="two columns"
                ),
                html.Div(id="CputEdge", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Bid Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="putBidAsym", type="text")], className="two columns"
                ),
                html.Div(id="CputBidAsym", className="two columns"),
            ],
            className="row",
        ),
        html.Div(
            [
                html.Div("Ask Asym", className="two columns"),
                html.Div(
                    [dcc.Input(id="putAskAsym", type="text")], className="two columns"
                ),
                html.Div(id="CputAskAsym", className="two columns"),
            ],
            className="row",
        ),
    ],
    className="row",
)

layout = html.Div(
    [
        topMenu("Settings"),
        html.Div(id="hidden-divSettings", style={"display": "none"}),
        html.Div(id="hidden-divSettings1", style={"display": "none"}),
        html.Div([dcc.Link("Home", href="/")], className="row"),
        productDropdown,
        inputs,
        html.Button("Submit", id="submit"),
    ]
)


def initialise_callbacks(app):
    # load vola data
    @app.callback(
        Output("hidden-divSettings", "children"),
        [Input("submit", "n_clicks")],
        state=[
            State(component_id="size", component_property="value"),
            State(component_id="product", component_property="value"),
            State(component_id="volEdge", component_property="value"),
            State(component_id="skewEdge", component_property="value"),
            State(component_id="callEdge", component_property="value"),
            State(component_id="putEdge", component_property="value"),
            State(component_id="Csize", component_property="children"),
            State(component_id="CvolEdge", component_property="children"),
            State(component_id="CskewEdge", component_property="children"),
            State(component_id="CcallEdge", component_property="children"),
            State(component_id="CputEdge", component_property="children"),
            State(component_id="volAskAsym", component_property="value"),
            State(component_id="skewAskAsym", component_property="value"),
            State(component_id="callAskAsym", component_property="value"),
            State(component_id="putAskAsym", component_property="value"),
            State(component_id="CvolAskAsym", component_property="children"),
            State(component_id="CskewAskAsym", component_property="children"),
            State(component_id="CcallAskAsym", component_property="children"),
            State(component_id="CputAskAsym", component_property="children"),
            State(component_id="volBidAsym", component_property="value"),
            State(component_id="skewBidAsym", component_property="value"),
            State(component_id="callBidAsym", component_property="value"),
            State(component_id="putBidAsym", component_property="value"),
            State(component_id="CvolBidAsym", component_property="children"),
            State(component_id="CskewBidAsym", component_property="children"),
            State(component_id="CcallBidAsym", component_property="children"),
            State(component_id="CputBidAsym", component_property="children"),
        ],
    )
    def submitVols(
        n_clicks,
        size,
        product,
        volEdge,
        skewEdge,
        callEdge,
        putEdge,
        Csize,
        CvolEdge,
        CskewEdge,
        CcallEdge,
        CputEdge,
        volAskAsym,
        skewAskAsym,
        callAskAsym,
        putAskAsym,
        CvolAskAsym,
        CskewAskAsym,
        CcallAskAsym,
        CputAskAsym,
        volBidAsym,
        skewBidAsym,
        callBidAsym,
        putBidAsym,
        CvolBidAsym,
        CskewBidAsym,
        CcallBidAsym,
        CputBidAsym,
    ):
        size = settingsUpdateCheck(size, Csize)

        volEdge = settingsUpdateCheck(volEdge, CvolEdge)
        skewEdge = settingsUpdateCheck(skewEdge, CskewEdge)
        callEdge = settingsUpdateCheck(callEdge, CcallEdge)
        putEdge = settingsUpdateCheck(putEdge, CputEdge)

        volAskAsym = settingsUpdateCheck(volAskAsym, CvolAskAsym)
        skewAskAsym = settingsUpdateCheck(skewAskAsym, CskewAskAsym)
        callAskAsym = settingsUpdateCheck(callAskAsym, CcallAskAsym)
        putAskAsym = settingsUpdateCheck(putAskAsym, CputAskAsym)

        volBidAsym = settingsUpdateCheck(volBidAsym, CvolBidAsym)
        skewBidAsym = settingsUpdateCheck(skewBidAsym, CskewBidAsym)
        callBidAsym = settingsUpdateCheck(callBidAsym, CcallBidAsym)
        putBidAsym = settingsUpdateCheck(putBidAsym, CputBidAsym)

        settings = {
            "savedDate": str(dt.datetime.now()),
            "size": size,
            "volEdge": volEdge,
            "skewEdge": skewEdge,
            "callEdge": callEdge,
            "putEdge": putEdge,
            "volAskAsym": volAskAsym,
            "skewAskAsym": skewAskAsym,
            "callAskAsym": callAskAsym,
            "putAskAsym": putAskAsym,
            "volBidAsym": volBidAsym,
            "skewBidAsym": skewBidAsym,
            "callBidAsym": callBidAsym,
            "putBidAsym": putBidAsym,
        }
        if n_clicks != None:
            sumbiSettings(product.lower(), settings)

    @app.callback(
        Output("hidden-divSettings1", "children"),
        [Input("submit", "n_clicks"), Input("product", "value")],
    )
    def updateSettings(interval, product):
        return retriveSettings(product.lower())

    def loadCurerntSettings(params, param):
        if params != None:
            params = json.loads(params)
            return str(params[param])
        else:
            return " "

    def createLoadVol(input):
        def loadVol(interval, params):
            return loadCurerntSettings(params, "{}".format(input[1:]))

        return loadVol

    # create callbacks for each input
    for param in (
        "Csize",
        "CvolEdge",
        "CskewEdge",
        "CcallEdge",
        "CputEdge",
        "CvolAskAsym",
        "CskewAskAsym",
        "CputAskAsym",
        "CcallAskAsym",
        "CvolBidAsym",
        "CskewBidAsym",
        "CputBidAsym",
        "CcallBidAsym",
    ):
        app.callback(
            Output(component_id="{}".format(param), component_property="children"),
            [Input("submit", "n_clicks"), Input("hidden-divSettings1", "children")],
        )(createLoadVol(param))

    def createBlank():
        def blankout(product, clicks):
            return ""

        return blankout

    # create callbacks to blank each input
    for param in (
        "size",
        "volEdge",
        "skewEdge",
        "callEdge",
        "putEdge",
        "volAskAsym",
        "skewAskAsym",
        "putAskAsym",
        "callAskAsym",
        "volBidAsym",
        "skewBidAsym",
        "putBidAsym",
        "callBidAsym",
    ):
        app.callback(
            Output(component_id="{}".format(param), component_property="value"),
            [Input("product", "value"), Input("submit", "n_clicks")],
        )(createBlank())
