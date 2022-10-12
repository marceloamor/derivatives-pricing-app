from int_app import create_app


app, server = create_app()


if __name__ == "__main__":
    # Ininlise app and server
    app.run(debug=True)
    server = app.server
