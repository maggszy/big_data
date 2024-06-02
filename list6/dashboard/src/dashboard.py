import json

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, State, callback, dcc, html


class Dashboard:
    def __init__(self, stock_name: str) -> None:
        self.title = stock_name
        self.app = Dash(prevent_initial_callbacks="initial_duplicate")
        self.app.title = f"Real-time {self.title} dashboard"

        self.app.layout = self.create_layout()
        self.server_data = None
        self.register_callbacks()

    def create_layout(self):
        invisible_children = [
            dcc.Store(id="data-store", storage_type="memory"),
            dcc.Interval(
                id="interval-component",
                interval=5 * 1000,
                n_intervals=0,
            ),
        ]

        structural_children = [
            html.H1(children=f"Real-time {self.title} dashboard"),
            html.Div(
                id="output-div",
                children="Loading...",
            ),
            dcc.Graph(id="line-chart"),
        ]

        return html.Div(children=invisible_children + structural_children)

    def register_callbacks(self):
        @callback(
            Output("data-store", "data"),
            [Input("interval-component", "n_intervals")],
            [State("data-store", "data")],
        )
        def update_data_store(n, data_dict):
            if hasattr(self.app, "server_data") and self.app.server_data != data_dict:
                return self.app.server_data
            return data_dict

        @callback(Output("output-div", "children"), [Input("data-store", "data")])
        def update_output_div(data_dict):
            if data_dict is None:
                return "Waiting for data... it might take a while."
            return ""

        @callback(Output("line-chart", "figure"), [Input("data-store", "data")])
        def update_output_div(data_dict):
            if data_dict is None:
                return None

            df = pd.DataFrame(data_dict)
            fig = px.line(
                df,
                x="timestamp",
                y=["price", "price_MA"],
                title=f"Line Chart of x",
            )
            return fig

    def run(self):
        self.app.run_server(debug=False, host="0.0.0.0")

    def call_refresh(self, data_bytes):
        with self.app.server.app_context():
            data_decoded = data_bytes.decode("utf-8")
            self.app.server_data = json.loads(data_decoded)
