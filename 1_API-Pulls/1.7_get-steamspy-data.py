## Step 7: Query SteamSpy to Retrieve Tags Data & Add it to steam_games_cleaned_languages.json
        # SteamSpy API query process must be altered.
        # Testing only returns data for appid 730, Counter-Strike: Global Offensive regardless of appid queried.


# Step 7.1: Create .csv file of current games for tracking purposes
# ??? Should this step exist? Should use the original steam_valid_apps_unique.csv to ensure consistency.
    # Especially if CSV is derived from grandparent file "steam_valid_apps_unique.csv"
import json
import pandas as pd

# File paths
INPUT_JSON = "steam_games_cleaned_languages.json"  # Source JSON
OUTPUT_CSV = "steamspy_games.csv"  # List of games to query

def extract_games_for_steamspy_csv():
    """Extracts steam_appid and name from JSON and saves to CSV."""
    try:
        # Load JSON data
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            games_data = json.load(f)

        # Convert to DataFrame
        df = pd.DataFrame(games_data, columns=["steam_appid", "name"])

        # Save to CSV
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Extracted {len(df)} games. Saved to {OUTPUT_CSV}")

    except Exception as e:
        print(f"Error processing JSON: {e}")

# Run function
extract_games_for_steamspy_csv()
# Step 7.2: Query SteamSpy API, Save Data, and Ensure All Games Were Queried
# Run in a separate Jupyter Notebook
import requests
import json
import pandas as pd
import time
import os

# API URL
    # URL for SteamSpy API must be verified. Testing only returns data for app 730: Counter-Strike: Global Offensive regardless of appid queried.
STEAMSPY_API_URL = "https://steamspy.com/api.php?request=appdetails&appid={}"

# File paths
INPUT_CSV = "steamspy_games.csv"  # List of games to query
PROCESSED_CSV = "steamspy_processed_games.csv"  # Track completed queries
OUTPUT_JSON = "steam_games_allclean_tags.json"  # Save cleaned API responses

# SteamSpy Rate Limits
REQUEST_INTERVAL = 1  # 1 request per second

# Keys to extract from API response
    # Keys of interest can be altered here
STEAMSPY_KEYS = ["positive", "negative", "owners", "median_forever", "median_2weeks", "ccu", "tags"]

def load_processed_games():
    """Loads processed game appids from CSV to avoid duplicate queries."""
    if os.path.exists(PROCESSED_CSV):
        try:
            df = pd.read_csv(PROCESSED_CSV)
            return set(df["steam_appid"])
        except Exception as e:
            print(f"Error reading processed games CSV: {e}")
    return set()

def save_processed_game(appid, name):
    """Logs processed games to CSV."""
    df = pd.DataFrame([[appid, name]], columns=["steam_appid", "name"])
    if not os.path.exists(PROCESSED_CSV):
        df.to_csv(PROCESSED_CSV, index=False)
    else:
        df.to_csv(PROCESSED_CSV, mode="a", index=False, header=False)

def fetch_steamspy_data(appid):
    """Fetches SteamSpy data for a given appid."""
    url = STEAMSPY_API_URL.format(appid)
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code} for appid {appid}")
    except requests.RequestException as e:
        print(f"Network error fetching {appid}: {e}")
    return None

def save_to_json(data, filename):
    """Appends new SteamSpy data to JSON file."""
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        else:
            existing_data = []

        existing_appids = {entry["steam_appid"] for entry in existing_data}
        new_entries = [entry for entry in data if entry["steam_appid"] not in existing_appids]

        existing_data.extend(new_entries)

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=4, ensure_ascii=False)

        print(f"{len(new_entries)} new games added to {filename}")

    except Exception as e:
        print(f"Error saving JSON: {e}")

def query_steamspy():
    """Queries SteamSpy API for all games listed in steamspy_games.csv."""
    games_df = pd.read_csv(INPUT_CSV)
    processed_games = load_processed_games()

    new_data = []
    for index, row in games_df.iterrows():
        appid, name = row["steam_appid"], row["name"]

        if appid in processed_games:
            print(f"Skipping already processed appid: {appid}")
            continue

        print(f"Fetching data for {appid} - {name}")
        game_data = fetch_steamspy_data(appid)

        if game_data:
            filtered_data = {key: game_data.get(key, None) for key in STEAMSPY_KEYS}
            filtered_data["steam_appid"] = appid  # Ensure appid is included
            new_data.append(filtered_data)

            # Log processed game
            save_processed_game(appid, name)

        # Respect API rate limit
        time.sleep(REQUEST_INTERVAL)

    # Save all new data
    if new_data:
        save_to_json(new_data, OUTPUT_JSON)
    print("SteamSpy data collection complete.")

# Run the function
query_steamspy()

# Ensure All Steam Apps Were Processed by Comparing steamspy_games.csv and steamspy_processed_games.csv
# File paths 
VALID_APPS_CSV = "steamspy_games.csv"  # Apps that should have been processed
PROCESSED_APPS_CSV = "steamspy_processed_games.csv"  # Apps that were actually processed
MISSING_APPS_CSV = "steamspy_missing_apps.csv"  # (Optional) Output file for missing apps

# Run the comparison
find_missing_apps()

## Step 7.3: Ensure SteamSpy JSON data is combined with Steam AppDetails API JSON
    # ??? This was clearly completed in the original project as the data is incorporated in "PROJECT_PHASE2.sqlite". However it isn't documented anywhere in the .ipynb files.