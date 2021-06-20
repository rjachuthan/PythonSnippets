import json
import pandas as pd
from datetime import datetime

username = "billybonaros"
js = json.load(open(f"{username}/{username}.json", encoding="utf-8"))

df = pd.DataFrame(js["GraphImages"])
df["likes"] = df["edge_media_preview_like"].apply(lambda x: x["count"])
df["comments"] = df["edge_media_to_comment"].apply(lambda x: x["count"])
df["date"] = df["taken_at_timestamp"].apply(datetime.fromtimestamp)
df["dayofweek"] = df["date"].dt.dayofweek
df["month"] = df["date"].dt.month
df["week"] = df["date"].dt.week
df["year"] = df["date"].dt.year
df["ym"] = df["year"].astype(str) + df["month"].astype(str)
df["dayofweek"] = df["dayofweek"].replace([0, 1, 2, 3, 4, 5, 6], ["Mon.", "Tue.", "Wed.", "Thu.", "Fri.", "Sat.", "Sun."])

x = df.groupby("dateofweek").size()
print(x.head())
