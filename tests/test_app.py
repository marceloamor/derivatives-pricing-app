# import dash
# import sys

# from dash.testing.application_runners import import_app
# from dash.testing.wait import until

# sys.path.append("src/")
# import app
# from dash.testing.application_runners import import_app
# import pytest


# leaving this code here for now, not currently working but may be useful later
# instantiating the app object works, but the dash specific functionality
# that is supposed to be available to the app object is not consistent w documentation

# the app object is indirectly but effectively tested in test_data_connections.py
# through the test of the app object's connection to the database via Flask SQLAlchemy


# ----------------------------------------
# from src.app import app  # Import your app's main Dash instance

# app_to_test = import_app("src.app.app")  # import the app variable from your app.py


# @pytest.fixture(scope="module")
# def dash_app():
#     yield app.create_app()


# def test_some_callback():
#     app.start_server(app)
#     app, server = app.create_app

# # Simulate user interaction and trigger the callback
# app_to_test.wait_for_element("#input-element")
# app_to_test.driver.find_element("#input-element").send_keys("test_input")
# app_to_test.wait_for_element("#output-element")

# # Assert the expected output
# output = app_to_test.find_element("#output-element")
# assert output.text == "Expected output"


# def test_dash_app(dash_threaded):
#     app = import_app("app")
#     dash_threaded(app)

#     # Use the `until` function to wait until the component is present
#     until(lambda: app.find_element("my-component"), timeout=5)


# def test_app_starts_up():
#     app = import_app("app")  # Import your app's main Dash instance
#     dash_app = dash.Dash(__name__)
#     dash_app.layout = app.layout

#     # Start the app
#     dash_app.run_server(debug=True, port=8050, host="localhost")

#     # Use the `until` function to wait until a specific element is present
#     until(lambda: dash_app.find_element("#input-element"), timeout=5)

#     # Assert that the app is running
#     assert dash_app.find_element("#input-element") is not None

#     # Close the app
#     dash_app.server.stop()
