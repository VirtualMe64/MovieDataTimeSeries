from utils import load_data
import json
import pandas as pd
import numpy as np

DATA = load_data("outputs/combined.csv")
IDS = json.load(open("ids.json", "r")) # see preprocessing.py for generation
TIME_VARIANT_CUTOFF = 69
OUT_FILE = "outputs/daily.csv"

first_write = True

daily_df = pd.DataFrame(columns=["DATE"] + DATA.columns.to_list(), index=IDS)
daily_df.drop(columns=["IMDB_ID"], inplace=True)
daily_df.index.name = "IMDB_ID"

data_cols = [x for x in DATA.columns.to_list() if x not in ["DATE_SCRAPE", "IMDB_ID", "TIME_SCRAPE", "WEEK", "CHAIN_URL", "SOURCE_URL"]]
agg_map = {x : "mean" for x in data_cols}
agg_map["TIME_SCRAPE"] = "last"
agg_map["WEEK"] = "last"
agg_map["CHAIN_URL"] = "last"
agg_map["SOURCE_URL"] = "last"

print("START")

grouped = DATA.groupby(by=["DATE_SCRAPE", "IMDB_ID"]).agg(agg_map)

print("END")

# iterate over each day
for date, df in grouped.groupby(level=0):
    print(date)
    
    df = df.droplevel(0)
    df.insert(0, "DATE_SCRAPE", date)
    
    # round all data to nearest integer
    df.loc[:, data_cols] = df.loc[:, data_cols].apply(lambda x : np.round(x))
    
    df.update(daily_df, overwrite=False)
    daily_df.update(df)
    daily_df["DATE"] = date
    daily_df.to_csv(OUT_FILE, mode='w' if first_write else 'a', header=first_write)
    first_write=False