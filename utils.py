import json
import pandas as pd
WEEK_KEY = "WEEK"
DATE_KEY = "DATE_SCRAPE"
TIME_KEY = "TIME_SCRAPE"

field_map = json.load(open("field_map.json", "r"))
dtypes = {x : float for x in field_map.values()}
dtypes['IMDB_ID'] = str
dtypes["CHAIN_URL"] = str
dtypes["SOURCE_URL"] = str
dtypes[WEEK_KEY] = str
dtypes[DATE_KEY] = str
dtypes[TIME_KEY] = str

def load_data(path : str):
    return pd.read_csv(path, na_values=["-"], thousands=',', dtype=dtypes)

def filter_na(df : pd.DataFrame, pct : int, stats : list):
    """
    filters out rows that have less than pct% of their values valid
    :param df: dataframe to filter
    :param pct: percentage of values that must be valid
    :param stats: list of stats to check
    """
    data_copy = df.copy()[stats + ["IMDB_ID"]]
    data_copy[stats] = (~data_copy[stats].isnull()).astype(int)
    
    grouped = data_copy.groupby("IMDB_ID")[stats]
    valid_count = grouped.sum().sum(axis = 1)
    count = grouped.count().sum(axis = 1)
    
    valids = valid_count / count
    valids = valids[valids <= pct].index
    return df[~df["IMDB_ID"].isin(valids)]

if __name__ == "__main__":
    data = load_data("outputs/combined.csv")
    stats = ["RATING01","RATING05","RATING10", "DEMO_US_RATING01", "DEMO_US_RATING05", "DEMO_US_RATING10"]
    
    n1 = 0.2
    n2 = 0.1
    
    movies1 = list(filter_na(data, n1, stats)["IMDB_ID"].unique())
    movies2 = list(filter_na(data, n2, stats)["IMDB_ID"].unique())
    
    print([x for x in movies2 if x not in movies1])
    
    '''
    import matplotlib.pyplot as plt
    
    data = load_data("outputs/combined.csv")
    stats = ["DATE_SCRAPE", "TIME_SCRAPE", "IMDB_ID", "RATING01","RATING05","RATING10", "DEMO_US_RATING01", "DEMO_US_RATING05", "DEMO_US_RATING10"]
    
    data["DATE_SCRAPE"] = pd.to_datetime(data["DATE_SCRAPE"], format="%Y-%m-%d")
    grouped = data[stats].dropna(how="any").groupby("DATE_SCRAPE")["IMDB_ID"].nunique()
    plt.plot(grouped)
    plt.scatter(grouped.index, grouped.values)
    print(grouped.size)
    print(grouped.sort_values(ascending=False))
    print(grouped.sort_values(ascending=True))
    plt.show()
    '''