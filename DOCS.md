### Step 1: Combine csvs (`combine.py`)
- Iterate over folders then file in folders
- Load CSV, add date time info, remove invalid (file length incorrection)
    - 2018-04-02 00.00.00.csv, 2018-07-29 19.00.00.csv
- Save new data to one file (`combine.csv`)
- Optional: log changes in invariant data and save those changes to file

### Step 2: Extract daily info (`daily.py`)
- Group combined csv data by day and id
- Take avergae of non null data for each day/id, rounding to nearest even number
- For each day, save mean of day's data (rounded) for each stat for every id to one file (`daily.csv`)

### Known errors:
- 5/27/2018 has only 2 entries