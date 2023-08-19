from utils import load_data
import matplotlib.pyplot as plt
import pandas as pd

def track_id(data : pd.DataFrame, id : str, stat : str, drop_duplicates = True, plot_diff = False):
    data = data[data["IMDB_ID"] == id]
    data["DATE"] = pd.to_datetime(data["DATE"], format="%Y-%m-%d")
    data = data.set_index("DATE")
    
    if drop_duplicates:
        data = data[data[stat].notna()].drop_duplicates(keep="first", subset=["DATE_SCRAPE"])
    else:
        data = data.fillna(0)
    
    data["DIFF"] = data[stat].diff()
    col = "DIFF" if plot_diff else stat
    
    print(data.shape)
    print(data[col])
    
    plt.plot(data[col])
    plt.show()

data = load_data("outputs/daily.csv")
track_id(data, "tt4154796", "RATING10", True, False)
track_id(data, "tt4154796", "RATING10", True, True)
