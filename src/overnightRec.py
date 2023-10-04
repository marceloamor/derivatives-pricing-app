# # compare F2 and Georgia position differences overnight
# from sql import pullAllF2Position, pullAllPosition
# from parts import convertInstrumentName

# # import win32
# import pandas as pd

# from datetime import timedelta

# # pull f2 trades
# # todays date in F2 format
# yesterday = pd.datetime.now() - timedelta(days=1)
# yesterday = yesterday.strftime("%Y-%m-%d")
# gTrades = pullAllPosition(yesterday)

# # convert quanitity to int
# gTrades["quanitity"] = gTrades["quanitity"].astype(int)

# # filter for columns we want
# gTrades = gTrades[["instrument", "quanitity", "prompt"]]

# # add venue label
# gTrades["venue"] = "Georgia"

# # pull F2 Positions
# fTrades = pullAllF2Position(yesterday)


# # convert prompt to date
# fTrades["prompt"] = pd.to_datetime(fTrades["prompt"], dayfirst=True, format="%d/%m/%Y")

# # build instrument name from parts
# fTrades["instrument"] = fTrades.apply(convertInstrumentName, axis=1)
# # filter for columns we want
# fTrades = fTrades[["instrument", "quanitity", "prompt"]]

# # add venue label
# fTrades["venue"] = "F2"

# # concat and group then take only inputs with groups of 1
# all = pd.concat([gTrades, fTrades])
# # filter for columns we want
# all = all[["instrument", "quanitity", "venue"]]
# all = all.reset_index(drop=True)
# all_gpby = all.groupby(list(["instrument", "quanitity"]))
# idx = [x[0] for x in all_gpby.groups.values() if len(x) % 2 != 0]


# print(all.reindex(idx))


# # win32 not included in requirements.txt, legacy code left in however
# def email(Subject, body):
#     outlook = win32.Dispatch("outlook.application")
#     mail = outlook.CreateItem(0)
#     mail.To = "gareth.upe@sucfin.com"
#     mail.Subject = str(Subject)
#     mail.HTMLBody = body
#     mail.Send()
