from dash.dependencies import Input, Output
from dash import dcc
from dash import dcc, html
import dash_auth, os, time

# from baseLog import baseLog
from datetime import datetime

# rootLogger = baseLog('Georgia Frontend')

servePort = int(os.getenv("SERVE_PORT", default="8050"))
serveLocation = os.getenv("SERVE_LCOATION", default="0.0.0.0")
serveDebug = os.getenv("DEBUG", default="debug")
delay = int(os.getenv("DELAY", default=5))

# waitress server
from waitress import serve

# import different apps
from app import app
from apps import (
    strikeRiskNew,
    trades,
    app2,
    homepage,
    rates,
    portfolio,
    position,
    promptCurve,
    logPage,
    calculator,
    settings,
    pnl,
    riskMatrix,
    strikeRisk,
    whiteBoard,
    deltaVolas,
    rec,
    volMatrix,
    expiry,
    routeStatus,
    staticData,
    calendarPage,
    cashManager,
    dataDownload,
    calculatorEUR,
)
import volSurfaceUI as volSurfaceUI

# Keep this out of source code repository - save in a file or a database
VALID_USERNAME_PASSWORD_PAIRS = [
    ["alan", "sucden2019"],
    ["gareth", "sucden2019"],
    ["raf", "sucden2019"],
    ["tom", "sucden2019"],
    ["cooey", "sucden2019"],
]
# authorise user
auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)

# add icon and title for top of website
# @app.server.route('/favicon.ico')
# def favicon():
#     return send_from_directory(os.path.join(server.root_path, 'assets/images'),
#                                'favicon.ico', mimetype='image/vnd.microsoft.icon')

app.title = "Georgia"

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        html.Div(list("ABC"), id="data", style={"display": "none"}),
        html.Div(id="page-content"),
    ]
)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname):
    if pathname == "/trades":
        return trades.layout
    elif pathname == "/app2":
        return app2.layout
    elif pathname == "/volsurface":
        return volSurfaceUI.layout
    elif pathname == "/rates":
        return rates.layout
    elif pathname == "/portfolio":
        return portfolio.layout
    elif pathname == "/position":
        return position.layout
    elif pathname == "/prompt":
        return promptCurve.layout
    elif pathname == "/logpage":
        return logPage.layout
    elif pathname == "/calculator":
        return calculator.layout
    elif pathname == "/settings":
        return settings.layout
    elif pathname == "/pnl":
        return pnl.layout
    elif pathname == "/riskmatrix":
        return riskMatrix.layout
    elif pathname == "/strikeRisk":
        return strikeRisk.layout
    elif pathname == "/volMatrix":
        return volMatrix.layout
    elif pathname == "/deltaVola":
        return deltaVolas.layout
    elif pathname == "/rec":
        return rec.layout
    elif pathname == "/expiry":
        return expiry.layout
    elif pathname == "/routeStatus":
        return routeStatus.layout
    elif pathname == "/staticData":
        return staticData.layout
    elif pathname == "/calendarPage":
        return calendarPage.layout
    elif pathname == "/cashManager":
        return cashManager.layout
    elif pathname == "/dataDownload":
        return dataDownload.layout
    elif pathname == "/calculatorEUR":
        return calculatorEUR.layout
    elif pathname == "/strikeRiskNew":
        return strikeRiskNew.layout
    else:
        return homepage.layout


if __name__ == "__main__":
    app.run(debug=True)
