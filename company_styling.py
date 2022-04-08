import os

comp_style = os.getenv("COMP_STYLE", default="upe")

if comp_style == "sfl":
    main_color = "primary"
    logo = "assets/images/favicon.ico"

if comp_style == "upe":
    main_color = "#2E2C68"
    logo = "assets/images/upe.ico"
    favicon_name = logo.split("/")[2]
