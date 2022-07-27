from int_app import create_app

# Ininlise app and server
app, server = create_app()

if __name__ == "__main__":

    app.run_server(debug=True)
    server = app.server
