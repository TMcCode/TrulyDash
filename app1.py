# Truly Dashboard Wrapup
# Addt'l Charts: 12pk Pricing over Time, 12pk Pricing over MSA

import psycopg2
import plotly.offline as pyo
import plotly.graph_objs as go
import plotly.express as px
import numpy as np
import pandas as pd
import plotly.figure_factory as ff
import sqlite3
from sqlite3 import Error
from urllib.request import urlopen
import json
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_auth
from plotly.subplots import make_subplots
import plotly.graph_objects as go

USERNAME_PASSWORD_PAIRS = [['username','password']]

from LineChart_MarketShare import (
    line_share_layout,
    line_share_data,
    line_share_fig
)

from LineChart_WeeklyMetrics_MultipleQueries import (
    line_growth_fig
)

from Chloro_ItemLevelSalesComparison_30DMA_byMSA import (
    chloro_salescomp_fig
)

from Chloro_ItemLevelGrowthMetrics_30DMA_byMSA import (
    chloro_growth_fig
)


# import Chloro_ItemLevelGrowthMetrics_30DMA_byMSA.py
# import Chloro_ItemLevelSalesComparison_30DMA_byMSA.py
# import LineChart_MarketShare.py
# import LineChart_WeeklyMetrics.py

# app = dash.Dash()
#
#
# app.layout = html.Div([dcc.Graph(id='lineshare',
#                                  figure = {'data':line_share_data,
#                                            'layout':line_share_layout}
#                                  ),
#                         dcc.Graph(id='linegrowth',
#                                  figure = {'data':line_growth_data,
#                                            'layout':line_growth_layout}
#                                   )
#                         ])
#
#
# if __name__ == '__main__':
#     app.run_server()
#
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
auth = dash_auth.BasicAuth(app, USERNAME_PASSWORD_PAIRS)
server = app.server

app.css.config.serve_locally = False
app.scripts.config.serve_locally = False

app.layout = html.Div([
    html.Div([
        html.Div([
            html.H3('Hard Seltzer Growth'),
            dcc.Graph(id='linegrowth',
                      figure=line_growth_fig
                      )
        ], className="six columns"),

        html.Div([
            html.H3('Weekly Hard Seltzer Market Share'),
            dcc.Graph(id='lineshare',
                      figure={'data': line_share_data,
                              'layout': line_share_layout}
                      )
        ], className="six columns"),
    ], className="row"),
    html.Div([
        html.Div([
            html.H3('Truly vs. Whiteclaw Orders by MSA (30DMA)'),
            dcc.Graph(id='mapvswhiteclaw',
                      figure=chloro_salescomp_fig
                      )
        ], className="six columns"),

        html.Div([
            html.H3('MSA Contribution to Truly Growth (30DMA)'),
            dcc.Graph(id='mapgrowthcontrib',
                      figure=chloro_growth_fig
                      )
        ], className="six columns"),
    ], className="row")
])

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(debug=True)

#
# fig = make_subplots(
#     rows=1, cols=2,
#     subplot_titles=("Plot 1", "Plot 2"))
#
# fig.add_trace(line_growth_data,
#               row=1, col=1)
#
# fig.add_trace(line_share_data,
#               row=1, col=2)
#
# fig.update_layout(height=500, width=700,
#                   title_text="Multiple Subplots with Titles")
#
#
# fig.show()