import pandas as pd
import matplotlib.pyplot as plt
import json

WEEK_KEY = "WEEK"
DATE_KEY = "DATE_SCRAPE"
TIME_KEY = "TIME_SCRAPE"
WEEK_FORMAT = "%Y%m%d"
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H.%M.%S"

with open("field_map.json", "r") as f:
    field_map = json.load(f)

def find_invalid_types(data, stat, correct_type_str = "float"):
    '''
    In the data set, coluimns 50-63 (M_RATING_M_U18-N_F_U18) excluding 59 had mixed types
    This function helped find the source of the error
    '''
    data['type'] = data[stat].apply(lambda x: type(x).__name__)
    res = data[data['type'] != correct_type_str]
    return res
    
def get_invalid_movie_ids(data, stats, threshhold = 0.5):
    '''
    Helps remove movies that have too many missing values
    Useful for making valid_pct plot reasonable
    If a movie has less than threshhold of its values present, it is in the returned index
    '''
    data_copy = data.copy()[stats + ["IMDB_ID"]]
    data_copy[stats] = (~data_copy[stats].isnull()).astype(int)
    
    grouped = data_copy.groupby("IMDB_ID")[stats]
    valid_count = grouped.sum().sum(axis = 1)
    count = grouped.count().sum(axis = 1)
    
    valids = valid_count / count
    
    return valids[valids <= threshhold].index

def plot_valid_pct_by(data, validation_stats, grouping_stat):
    data_copy = data.copy()[validation_stats + [grouping_stat]]
    data_copy[validation_stats] = (~data_copy[validation_stats].isnull()).astype(int)
    
    grouped = data_copy.groupby(grouping_stat)[validation_stats]
    valid_count = grouped.sum().sum(axis = 1)
    count = grouped.count().sum(axis = 1)

    valid_pct = valid_count / count
    
    plt.plot(valid_pct)
    plt.title(validation_stats[0])
    plt.ylim([0, 1.1])
    plt.show()

if __name__ == "__main__":
    DATA_PATH = "outputs/combined.csv" # input file with all variant data processed already
    
    dtypes = {x : float for x in field_map.values()}
    dtypes['IMDB_ID'] = str
    dtypes["CHAIN_URL"] = str
    dtypes["SOURCE_URL"] = str
    dtypes[WEEK_KEY] = str
    dtypes[DATE_KEY] = str
    dtypes[TIME_KEY] = str
    data = pd.read_csv(DATA_PATH, na_values=["-"], thousands=',', dtype=dtypes)
    
    print(data.shape)
    
    data['datetime'] = pd.to_datetime(data[DATE_KEY] + " " + data[TIME_KEY], format=DATE_FORMAT + " " + TIME_FORMAT)
    data[WEEK_KEY] = pd.to_datetime(data[WEEK_KEY], format=WEEK_FORMAT)
    data[DATE_KEY] = pd.to_datetime(data[DATE_KEY], format=DATE_FORMAT)
    data[TIME_KEY] = pd.to_datetime(data[TIME_KEY], format=TIME_FORMAT)
    
    # remove movies that are mostly missing
    invalid = get_invalid_movie_ids(data, ["RATING01"], 0)
    print(invalid)
    print(f"{invalid.size} movies removed out of {data['IMDB_ID'].nunique()}")
    data = data[~data["IMDB_ID"].isin(invalid)]
    
    plot_valid_pct_by(data, ["RATING01"], WEEK_KEY)
    plot_valid_pct_by(data, ["DEMO_NON_US_RATING01", "RATING01", "DEMO_US_RATING01"], WEEK_KEY)