from global_variables import APP_LIST_URL, VALID_APPS_CSV, VALID_APPS_UNIQUE_CSV

## Step 1: Query the ISteam API to Determine Total Number of Valid Steam Apps
# Depricated step: The ISteamApps/GetAppList method was replaced by IStoreService/GetAppList
# Replacement occured between FEB2025 and DEC2025
# ??? Improve: Modify Step to properly query functioning Steam API. fetch_and_save_valid_apps will change.
    # Ensure K-V pairs have same naming convention to not affect other functions

# import requests ???
# import pandas as pd ???

# Steam API URL (Replace 'STEAMKEY' with your API key)
    # .format method not used for API key as I didn't have one
    # ??? Improve: Add line to incorporate API key, but comment it out
        # APP_LIST_URL = "http://api.steampowered.com/ISteamApps/GetAppList/v0002/?key={STEAMKEY}&format=json"

def fetch_and_save_valid_apps():
    """Fetches all Steam apps and saves only those with valid names (!= null) to VALID_APPS_CSV."""
    try:
        response = requests.get(APP_LIST_URL)
        response.raise_for_status()
        all_apps_data = response.json()

        apps = all_apps_data.get("applist", {}).get("apps", [])
        #for loop only pulls data if "name" != None
        valid_apps = [{"appid": app["appid"], "name": app["name"]} for app in apps if app.get("name")]

        df = pd.DataFrame(valid_apps)
        df.to_csv(VALID_APPS_CSV, index=False)

        print(f"Saved {len(df)} valid apps to {VALID_APPS_CSV}")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching app list: {e}")

# Run step 1
    # run in main.py ???
fetch_and_save_valid_apps()


##################################################


## Step 2: Remove Duplicate Steam App IDs from the list of Valid Steam Apps

# import pandas as pd ???

# File paths
INPUT_CSV = VALID_APPS_CSV
OUTPUT_CSV = VALID_APPS_UNIQUE_CSV

def remove_csv_duplicates(input_csv, output_csv):
    """Removes duplicate steam_appid entries from a CSV file."""
    try:
        # Load CSV file
        df = pd.read_csv(input_csv)

        # Drop duplicates based on steam_appid, keeping the first occurrence
        df_unique = df.drop_duplicates(subset=["appid"], keep="first")

        # Save cleaned CSV file
        df_unique.to_csv(output_csv, index=False)

        print(f"Removed {(len(df))-(len(df_unique))} duplicates. Original count: {len(df)}, Unique count: {len(df_unique)}")
        print(f"Cleaned file saved as: {output_csv}")

    except FileNotFoundError:
        print("Error: CSV file not found.")
    except Exception as e:
        print(f"Error: {e}")

# Run the function
    # run in main.py ???
remove_csv_duplicates(INPUT_CSV, OUTPUT_CSV)