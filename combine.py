import os
import json
import time
import pandas as pd
from typing import List


# Constants
WEEK_KEY = "WEEK"
DATE_KEY = "DATE_SCRAPE"
TIME_KEY = "TIME_SCRAPE"
PATH = "./DataQualityFiles/"
FIELD_MAP = json.load(open("field_map.json", "r"))
IDS = json.load(open("ids.json", "r")) # see preprocessing.py for generation
DT_STRING = "%Y-%m-%d%H.%M.%S"
TIME_VARIANT_CUTOFF = 69

# CONFIG
DO_INVARIANT = False # whether to extract invariant data (likely only need to do this once)
DO_VARIANT = True # whether to extract variant data
INVARIANT_IGNORE = ["MPAA_IMG"] # columns to ignore when looking for invaraint data changes
OUT_FILE_INVARIANT = "invariant.csv" # output file name for invariant data
OUT_FILE_VARIANT = "outputs/combined.csv" # output file name for variant data

# instance variables
first_write = {}

invariant_df = pd.DataFrame(columns=[x for x in FIELD_MAP.values()][TIME_VARIANT_CUTOFF:], index=IDS)
invariant_df_copy = invariant_df.copy()

# change log takes in a list of unique ids, which requires processing the data once to get the ids
change_log = pd.DataFrame(columns=[x for x in FIELD_MAP.values()][TIME_VARIANT_CUTOFF:])
change_log.insert(loc=0, column="DATETIME", value="")
change_log.index.name = "IMDB_UD"

def handle_invariant(df : pd.DataFrame, date_time):
    # given a dataframe of invariant data, compares it to the previous invariant data
    # if there are changes, saves these changes to the change log
    global invariant_df_copy, invariant_df, change_log
    df = df.dropna()
    df = df[~df.index.duplicated(keep='first')]
    
    invariant_df_copy.update(df) # changes the values in invariant_df_copy to the values in df
    
    diff = invariant_df_copy.drop(INVARIANT_IGNORE, axis = 1).compare(invariant_df.drop(INVARIANT_IGNORE, axis=1))
    diff = invariant_df_copy.loc[diff.index] # finds row in invariant_df_copy that has changed
    
    if diff.shape[0] > 0:
        diff.insert(loc=0, column="DATETIME", value=date_time)
        change_log = pd.concat([change_log, diff])
    
    invariant_df = invariant_df_copy.copy()

def write_to_out(df, file):\
    # adds the data from the given df to a csv file
    # adds header and overwrites the first time, otherwise appends
    global first_write
    first = first_write.get(file, True)
    df.to_csv(
        file,
        mode='w' if first else 'a', # first file overwrites if boolean is set
        header=first, # only one header
    )
    first_write[file] = False
    
def process_file(full_path, file_name, folder_name):
    # given one CSV from the raw data, processes and saves it
    # renames columns, adds date and time, and seperates time variant and invariant data
    df = pd.read_csv(full_path, thousands=',')
    
    if (df.shape[1] < 80):
        # invalid file
        print(full_path)
        return
    
    date = file_name[29:39]
    time = file_name[40:48]
    
    df.rename(columns=FIELD_MAP, inplace=True)
    df.insert(loc=0, column=WEEK_KEY, value=folder_name)
    df.insert(loc=0, column=TIME_KEY, value=time)
    df.insert(loc=0, column=DATE_KEY, value=date)
    
    df.set_index("IMDB_ID", inplace=True, drop=False)
    
    
    if DO_INVARIANT:
        time_invariant = df.iloc[:, TIME_VARIANT_CUTOFF + 3:] # the remaining (variant) columns (skipping IMDB_ID)
        handle_invariant(time_invariant, date + time)
    if DO_VARIANT:
        time_variant = df.iloc[:, :TIME_VARIANT_CUTOFF] # first [TIME_VARIANT_CUTOFF] columns
        write_to_out(time_variant, OUT_FILE_VARIANT)
  
total_folders = len(os.listdir(PATH))
start = time.time()

# iterate over all files
for i, folder in enumerate(os.listdir(PATH)):
    dt = time.time() - start
    pct = (i / total_folders)
    print(f"Processing folder: {folder}")
    print(f"Progress: {(pct):.2%}")
    print(f"Elapsed: {dt:.2f}s, ~Time Left: {dt / pct - dt if pct != 0 else 180:.2f}s")
    for file in os.listdir(PATH + folder): # iterates over all files in every folder
        full_path = PATH + folder + "/" + file
        process_file(full_path, file, folder)
        
change_log.sort_index(inplace=True, kind="stable") # sorts by IMDB_ID then DATETIME (since sort is stable)
change_log["DATETIME"] = pd.to_datetime(change_log["DATETIME"], format=DT_STRING)
change_log.to_csv(OUT_FILE_INVARIANT, index_label="IMDB_ID")