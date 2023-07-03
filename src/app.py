from int_app import create_app
from flask_sqlalchemy import SQLAlchemy
import os
import pandas as pd

from data_connections import db




app, server = create_app()


# connect database to app
postgresURL = os.environ.get("GEORGIA_POSTGRES_URL")
app.server.config["SQLALCHEMY_DATABASE_URI"] = postgresURL
# necessary to suppress warning when using flask_sqlalchemy
app.server.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# db.init_app(app.server)

# from routes import routes
# routes(app, server)

# #db = SQLAlchemy(server)
# df = pd.read_sql_table('products', con=db.engine)
# print(df)


if __name__ == "__main__":
    # Ininlise app and server
    app.run(debug=True)
    server = app.server
