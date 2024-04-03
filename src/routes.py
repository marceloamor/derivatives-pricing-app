import os

# import volSurfaceUI as volSurfaceUI
from apps import (
    #     deltaVolas,
    # expiry,
    calculator2,
    #     calculator,
    #     calculatorEUR,
    calendarPage,
    cashManager,
    dataDownload,
    dataLoad,
    homepage,
    lme_carry,
    #     logPage,
    # m2m_rec,
    #     pnl,
    portfolio,
    position,
    #     promptCurve,
    #     rates,
    #     riskMatrix,
    # routeStatus,
    rec,
    staticData,
    #     strikeRisk,
    strikeRiskNew,
    trades,
    vol_matrix_new,
    volMatrix,
)
from company_styling import favicon_name
from dash.dependencies import Input, Output
from flask import send_from_directory


def routes(app, server):
    # initialise callbacks for all the pages
    # volSurfaceUI.initialise_callbacks(app)
    dataLoad.initialise_callbacks(app)
    trades.initialise_callbacks(app)
    lme_carry.initialise_callbacks(app)
    homepage.initialise_callbacks(app)
    # rates.initialise_callbacks(app)
    portfolio.initialise_callbacks(app)
    position.initialise_callbacks(app)
    vol_matrix_new.initialise_callbacks(app)
    # promptCurve.initialise_callbacks(app)
    # logPage.initialise_callbacks(app)
    # pnl.initialise_callbacks(app)
    # riskMatrix.initialise_callbacks(app)
    # strikeRisk.initialise_callbacks(app)
    strikeRiskNew.initialise_callbacks(app)
    # deltaVolas.initialise_callbacks(app)
    rec.initialise_callbacks(app)

    # rec.initialise_callbacks(app)
    calculator2.initialise_callbacks(app)
    volMatrix.initialise_callbacks(app)
    # expiry.initialise_callbacks(app)
    # routeStatus.initialise_callbacks(app)
    calendarPage.initialise_callbacks(app)
    cashManager.initialise_callbacks(app)
    dataDownload.initialise_callbacks(app)
    staticData.initialise_callbacks(app)
    # m2m_rec.initialise_callbacks(app)

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
        # elif pathname == "/volsurface":
        #     return volSurfaceUI.layout
        # elif pathname == "/rates":
        #     return rates.layout
        elif pathname == "/portfolio":
            return portfolio.layout
        elif pathname == "/position":
            return position.layout
        elif pathname == "/volMatrixNew":
            return vol_matrix_new.layout
        elif pathname == "/calculator2":
            return calculator2.layout
        # elif pathname == "/prompt":
        #     return promptCurve.layout
        # elif pathname == "/logpage":
        #     return logPage.layout
        # elif pathname == "/calculator":
        #     return calculator.layout
        # elif pathname == "/pnl":
        #     return pnl.layout
        # elif pathname == "/riskmatrix":
        #     return riskMatrix.layout
        # elif pathname == "/strikeRisk":
        #     return strikeRisk.layout
        elif pathname == "/volMatrix":
            return volMatrix.layout
        # elif pathname == "/deltaVola":
        #     return deltaVolas.layout
        elif pathname == "/rec":
            return rec.layout
        # elif pathname == "/expiry":
        #     return expiry.layout
        # elif pathname == "/routeStatus":
        #     return routeStatus.layout
        elif pathname == "/staticData":
            return staticData.layout
        # # elif pathname == "/brokers":
        # #     return brokers.layout
        elif pathname == "/dataload":
            return dataLoad.layout
        elif pathname == "/calendarPage":
            return calendarPage.layout
        elif pathname == "/cashManager":
            return cashManager.layout
        elif pathname == "/dataDownload":
            return dataDownload.layout
        # elif pathname == "/calculatorEUR":
        #     return calculatorEUR.layout
        elif pathname == "/strikeRiskNew":
            return strikeRiskNew.layout
        elif pathname == "/lmecarry":
            return lme_carry.layout
        # elif pathname == "/m2m_rec":
        #     return m2m_rec.layout

        else:
            return homepage.layout
