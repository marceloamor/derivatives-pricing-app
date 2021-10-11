from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc

from app import app

form = html.Div(['this is a form'])
title = html.Div([html.H3('this is title')])

layout = html.Div([
    title,
    form
])

