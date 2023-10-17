import dash
import sys

sys.path.append("src/app.py")
from app import app
from dash.testing.application_runners import import_app
import pytest

# from src.app import app  # Import your app's main Dash instance

# app_to_test = import_app("src.app.app")  # import the app variable from your app.py


# @pytest.fixture(scope="module")
# def dash_app():
#     yield app


# def test_some_callback(dash_app):
#     app_to_test.start_server(app)

#     # Simulate user interaction and trigger the callback
#     app_to_test.wait_for_element("#input-element")
#     app_to_test.driver.find_element("#input-element").send_keys("test_input")
#     app_to_test.wait_for_element("#output-element")

#     # Assert the expected output
#     output = app_to_test.find_element("#output-element")
#     assert output.text == "Expected output"
