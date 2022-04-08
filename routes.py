from apps import (
    dataLoad,
    brokers,
    trades,
    homepage,
    rates,
    portfolio,
    position,
    promptCurve,
    logPage,
    calculator,
    pnl,
    riskMatrix,
    strikeRisk,
    deltaVolas,
    rec,
    volMatrix,
    expiry,
    routeStatus,
    staticData,
)
import volSurfaceUI
from dash.dependencies import Input, Output
from company_styling import favicon_name
from riskapi import runRisk
from flask import request, send_from_directory
import os


def routes(app, server):

    # initialise callbacks for all the pages
    volSurfaceUI.initialise_callbacks(app)
    dataLoad.initialise_callbacks(app)
    brokers.initialise_callbacks(app)
    trades.initialise_callbacks(app)
    homepage.initialise_callbacks(app)
    rates.initialise_callbacks(app)
    portfolio.initialise_callbacks(app)
    position.initialise_callbacks(app)
    promptCurve.initialise_callbacks(app)
    logPage.initialise_callbacks(app)
    calculator.initialise_callbacks(app)
    pnl.initialise_callbacks(app)
    riskMatrix.initialise_callbacks(app)
    strikeRisk.initialise_callbacks(app)
    deltaVolas.initialise_callbacks(app)
    rec.initialise_callbacks(app)
    volMatrix.initialise_callbacks(app)
    expiry.initialise_callbacks(app)
    routeStatus.initialise_callbacks(app)
    staticData.initialise_callbacks(app)

    # for Risk API
    @server.route("/RiskApi/V1/risk")
    def risk_route():
        portfolio = request.args.get("portfolio", default="*", type=str)
        vol = request.args.get("vol").split(",")
        und = request.args.get("und").split(",")
        level = request.args.get("level", default="high", type=str)
        eval = request.args.get("eval")
        rel = request.args.get("rel", default="abs", type=str)

        # default level back to high
        level = "high"
        ApiInputs = {
            "portfolio": portfolio,
            "vol": vol,
            "und": und,
            "level": level,
            "eval": eval,
            "rel": rel,
        }
        try:
            return runRisk(ApiInputs)
        except Exception as e:
            print("RISK_API: Failed to calculate risk {}".format(str(e)))

    # add icon and title for top of website
    @app.server.route("/favicon.ico")
    def favicon():
        return send_from_directory(
            os.path.join(server.root_path, "assets/images"),
            favicon_name,
            mimetype="image/vnd.microsoft.icon",
        )

    @app.callback(Output("page-content", "children"), [Input("url", "pathname")])
    def display_page(pathname):
        if pathname == "/trades":
            return trades.layout
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
        elif pathname == "/brokers":
            return brokers.layout
        elif pathname == "/dataload":
            return dataLoad.layout
        else:
            return homepage.layout
